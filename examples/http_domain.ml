let queue_mode = ref `Thread
let queues = ref 3
let port = ref 8080
let usage = "usage: http_domain [options]"
let ( let* ) = Duppy.Monad.bind

let () =
  let () =
    match Domain.recommended_domain_count () with
      | 1 -> queue_mode := `Thread
      | n ->
          queues := n - 1;
          queue_mode := `Domain
  in
  let arg _ =
    Printf.eprintf "Error: too many arguments\n";
    exit 1
  in
  Arg.parse
    [
      ( "--queues",
        Arg.Int (fun i -> queues := i),
        Printf.sprintf "Number of non-blocking queues. (default: %d)" !queues );
      ( "--mode",
        Arg.String
          (fun m ->
            match m with
              | "thread" -> queue_mode := `Thread
              | "domain" -> queue_mode := `Thread
              | v -> failwith ("Invalid queue mode: " ^ v)),
        Printf.sprintf "Queue mode. (default: %s)"
          (match !queue_mode with `Thread -> "thread" | `Domain -> "domain") );
      ( "--port",
        Arg.Int (fun i -> port := i),
        Printf.sprintf "Port used to bind the server. (default: %d)" !port );
    ]
    arg usage

type priority = Non_blocking

let scheduler = Duppy.create ()

type http_method = Post | Get
type http_protocol = Http_11 | Http_10

let string_of_protocol = function
  | Http_11 -> "HTTP/1.1"
  | Http_10 -> "HTTP/1.0"

let protocol_of_string = function
  | "HTTP/1.1" -> Http_11
  | "HTTP/1.0" -> Http_10
  | _ -> assert false

let string_of_method = function Post -> "POST" | Get -> "GET"

let method_of_string = function
  | "POST" -> Post
  | "GET" -> Get
  | _ -> assert false

type data = None | String of string

type request = {
  request_protocol : http_protocol;
  request_method : http_method;
  request_uri : string;
  request_headers : (string * string) list;
  request_data : data;
}

type reply = {
  reply_protocol : http_protocol;
  reply_status : int * string;
  reply_headers : (string * string) list;
  reply_data : data;
}

exception Assoc of string

let assoc_uppercase x y =
  try
    List.iter
      (fun (l, v) ->
        if String.uppercase_ascii l = x then raise (Assoc v) else ())
      y;
    raise Not_found
  with Assoc s -> s

let server = "dhttpd"

let html_template =
  Printf.sprintf
    "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" \
     \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\r\n\
     <html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">\r\n\
     %s</html>"

let server_error status protocol =
  let _, explanation = status in
  let data =
    String
      (html_template
         (Printf.sprintf "<head><title>%s</title></head>\r\n<body>%s !</body>"
            explanation explanation))
  in
  {
    reply_protocol = protocol;
    reply_status = status;
    reply_headers =
      [("Content-Type", "text/html; charset=UTF-8"); ("Server", server)];
    reply_data = data;
  }

let error_404 = server_error (404, "File Not Found")
let error_500 = server_error (500, "Bad Request") Http_10
let error_403 = server_error (403, "Forbidden")

let http_302 protocol uri =
  {
    reply_protocol = protocol;
    reply_status = (302, "Found");
    reply_headers = [("Location", uri)];
    reply_data = String "";
  }

let send_reply h reply =
  let write s =
    Duppy.Monad.Io.write ?timeout:None ~priority:Non_blocking h
      (Bytes.unsafe_of_string s)
  in
  let code, status = reply.reply_status in
  let http_header =
    Printf.sprintf "%s %d %s\r\n%s\r\n\r\n"
      (string_of_protocol reply.reply_protocol)
      code status
      (String.concat "\r\n"
         (List.map
            (fun (x, y) -> Printf.sprintf "%s: %s" x y)
            reply.reply_headers))
  in
  let* () = write http_header in
  match reply.reply_data with
    | String s -> write s
    | None -> Duppy.Monad.return ()

let parse_headers headers =
  let split_header l h =
    try
      let rex = Pcre.regexp "([^:\\r\\n]+):\\s*([^\\r\\n]+)" in
      let sub = Pcre.exec ~rex h in
      Duppy.Monad.return
        ((Pcre.get_substring sub 1, Pcre.get_substring sub 2) :: l)
    with Not_found -> Duppy.Monad.raise error_500
  in
  Duppy.Monad.fold_left split_header [] headers

let payload = String.init 4096 (fun i -> Char.chr (i mod 100))

let handle_request request =
  if request.request_uri = "/" then (
    let headers =
      [
        ("Server", server);
        ("Content-Length", string_of_int (String.length payload));
        ("Content-Type", "application/octet-stream");
      ]
    in
    Duppy.Monad.raise
      {
        reply_protocol = request.request_protocol;
        reply_status = (200, "OK");
        reply_headers = headers;
        reply_data = String payload;
      })
  else Duppy.Monad.return (error_404 request.request_protocol)

let parse_request h r =
  try
    let headers = Pcre.split ~pat:"\r\n" r in
    let* request, headers =
      match headers with
        | e :: l ->
            let* headers = parse_headers l in
            Duppy.Monad.return (e, headers)
        | _ -> Duppy.Monad.raise error_500
    in
    let rex = Pcre.regexp "([\\w]+)\\s([^\\s]+)\\s(HTTP/1.[01])" in
    let* http_method, uri, protocol =
      try
        let sub = Pcre.exec ~rex request in
        let http_method, uri, protocol =
          ( Pcre.get_substring sub 1,
            Pcre.get_substring sub 2,
            Pcre.get_substring sub 3 )
        in
        Duppy.Monad.return
          (method_of_string http_method, uri, protocol_of_string protocol)
      with _ -> Duppy.Monad.raise error_500
    in
    let* data =
      match http_method with
        | Get -> Duppy.Monad.return None
        | Post -> (
            let* len =
              try
                let length = assoc_uppercase "CONTENT-LENGTH" headers in
                Duppy.Monad.return (int_of_string length)
              with
                | Not_found -> Duppy.Monad.return 0
                | _ -> Duppy.Monad.raise error_500
            in
            match len with
              | 0 -> Duppy.Monad.return None
              | d ->
                  let* data =
                    Duppy.Monad.Io.read ?timeout:None ~priority:Non_blocking
                      ~marker:(Duppy.Io.Length d) h
                  in
                  Duppy.Monad.return (String data))
    in
    Duppy.Monad.return
      {
        request_method = http_method;
        request_protocol = protocol;
        request_uri = uri;
        request_headers = headers;
        request_data = data;
      }
  with _ -> Duppy.Monad.raise error_500

let handle_client socket =
  (* Read and process lines *)
  let on_error _ = error_500 in
  let h = { Duppy.Monad.Io.scheduler; socket; data = ""; on_error } in
  let exec =
    let* reply =
      Duppy.Monad.catch
        (let* data =
           Duppy.Monad.Io.read ?timeout:None ~priority:Non_blocking
             ~marker:(Duppy.Io.Split "\r\n\r\n") h
         in
         let* request = parse_request h data in
         handle_request request)
        (fun reply -> Duppy.Monad.return reply)
    in
    send_reply h reply
  in
  let finish _ = try Unix.close socket with _ -> () in
  Duppy.Monad.run ~return:finish ~raise:finish exec

let new_queue ~priority ~name () =
  let priorities p = p = priority in
  let queue () = Duppy.queue scheduler ~log:(fun _ -> ()) ~priorities name in
  match !queue_mode with
    | `Thread -> `Thread (Thread.create queue ())
    | `Domain -> `Domain (Domain.spawn queue)

let bind_addr_inet = Unix.inet_addr_of_string "0.0.0.0"
let bind_addr = Unix.ADDR_INET (bind_addr_inet, !port)
let max_conn = 100
let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0

let () =
  (* See http://caml.inria.fr/mantis/print_bug_page.php?bug_id=4640
   * for this: we want Unix EPIPE error and not SIGPIPE, which
   * crashes the program.. *)
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  ignore (Unix.sigprocmask Unix.SIG_BLOCK [Sys.sigpipe]);
  Unix.setsockopt sock Unix.SO_REUSEADDR true;
  let rec incoming _ =
    (try
       let s, _ = Unix.accept sock in
       handle_client s
     with e ->
       Printf.printf "Failed to accept new client: %S\n" (Printexc.to_string e));
    [
      {
        Duppy.Task.priority = Non_blocking;
        events = [`Read sock];
        handler = incoming;
      };
    ]
  in
  (try Unix.bind sock bind_addr
   with Unix.Unix_error (Unix.EADDRINUSE, "bind", "") ->
     failwith (Printf.sprintf "port %d already taken" !port));
  Unix.listen sock max_conn;
  Duppy.Task.add scheduler
    {
      Duppy.Task.priority = Non_blocking;
      events = [`Read sock];
      handler = incoming;
    };
  for i = 1 to !queues do
    Printf.printf "Initiating queue %d\n%!" i;
    ignore
      (new_queue ~priority:Non_blocking
         ~name:(Printf.sprintf "Non blocking queue #%d" i)
         ())
  done;
  Duppy.queue scheduler ~log:(fun _ -> ()) "root"
