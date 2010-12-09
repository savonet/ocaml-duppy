(*pp $PP *)

let non_blocking_queues = ref 3
let maybe_blocking_queues = ref 1
let files_path = ref ""
let port = ref 8080

let usage = "usage: http [options] /path/to/files"

let () =
   let pnum = ref 0 in
   let arg s = 
     incr pnum;
     if !pnum > 1 then
       (Printf.eprintf "Error: too many arguments\n"; exit 1)
     else
       files_path := s
  in
  Arg.parse
    [
      "--non_blocking_queues", Arg.Int (fun i -> non_blocking_queues := i),
      (Printf.sprintf
        "Number of non-blocking queues. (default: %d)" !non_blocking_queues);
      "--maybe_blocking_queues", Arg.Int (fun i -> maybe_blocking_queues := i),
      (Printf.sprintf
        "Number of maybe-blocking queues. (default: %d)" !maybe_blocking_queues) ;
      "--port", Arg.Int (fun i -> port := i),
      (Printf.sprintf
        "Port used to bind the server. (default: %d)" !port) ;
    ] arg usage ;
  if !files_path = "" then
    (
      Printf.printf "%s\n" usage;
      exit 1
    )
  

type priority = 
     Maybe_blocking
   | Non_blocking

let scheduler = Duppy.create ()

type http_method = Post | Get

type http_protocol = Http_11 | Http_10

let string_of_protocol = 
   function
      | Http_11 -> "HTTP/1.1"
      | Http_10 -> "HTTP/1.0"

let protocol_of_string =
   function
      | "HTTP/1.1" -> Http_11
      | "HTTP/1.0" -> Http_10
      | _          -> assert false 

let string_of_method = 
   function
      | Post -> "POST"
      | Get  -> "GET" 

let method_of_string =
   function
      | "POST" -> Post
      | "GET"  -> Get
      | _      -> assert false

type data = None | String of string | File of Unix.file_descr

type request = 
  { request_protocol : http_protocol ;
    request_method   : http_method ;
    request_uri      : string ;
    request_headers  : (string*string) list ;
    request_data     : data }

type reply = 
  { reply_protocol : http_protocol ;
    reply_status   : int*string ;
    reply_headers  : (string*string) list ;
    reply_data     : data }

let date t = 
  Printf.sprintf
    "%02d-%02d-%04d %02d:%02d:%02d"
    t.Unix.tm_mday
    (t.Unix.tm_mon + 1)
    (1900 + t.Unix.tm_year)
    t.Unix.tm_hour
    t.Unix.tm_min
    t.Unix.tm_sec

exception Assoc of string

let assoc_uppercase x y =
  try
    List.iter
      (fun (l,v) -> if String.uppercase l = x then
                     raise (Assoc v)) y ;
    raise Not_found
  with
    | Assoc s -> s

let server = "dhttpd"

let html_template = 
  Printf.sprintf
     "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" \
     \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\r\n\
     <html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">\r\n\
     %s</html>"

let server_error status protocol =
  let (code,explanation) = status in
  let data = 
    String
      (html_template
        (Printf.sprintf 
          "<head><title>%s</title></head>\r\n\
           <body>%s !</body>" explanation explanation))
  in
  { reply_protocol = protocol ;
    reply_status   = status ;
    reply_headers  = [ "Content-Type","text/html; charset=UTF-8";
                       "Date", date (Unix.gmtime (Unix.gettimeofday ()));
                       "Server", server ] ;
    reply_data     = data } 

let error_404 = 
  server_error (404,"File Not Found")

let error_500 =
   server_error (500,"Bad Request") Http_10

let error_403 = 
  server_error (403,"Forbidden")

let send_reply h reply =
  let write s = 
    duppy_write 
      s
    with
      { priority = Non_blocking ;
        handler  = h }
  in
  let (code,status) = reply.reply_status in
  let http_header = 
    Printf.sprintf 
      "%s %d %s\r\n\
       %s\r\n\
       \r\n"
       (string_of_protocol reply.reply_protocol) code status
       (String.concat "\r\n" 
         (List.map 
           (fun (x,y) -> 
             Printf.sprintf "%s: %s" x y) reply.reply_headers))
  in
  duppy_try
    duppy_do
      write http_header ;
      begin
       match reply.reply_data with
         | String s -> 
             write s
         | File fd -> 
             let stats = Unix.fstat fd in
             let ba = 
               Bigarray.Array1.map_file
                 fd Bigarray.char Bigarray.c_layout false
                 (stats.Unix.st_size)
             in
             let close () = 
               try
                 Unix.close fd
               with
                 | _ -> ()
             in
             let on_error e = 
                close () ;
                h.Duppy.Monad.Io.on_error e
             in
             let h = 
               { h with
                  Duppy.Monad.Io.
                   on_error = on_error }
             in
             duppy_do
               duppy_write_bigarray 
                 ba
               with
                 { priority = Non_blocking ;
                   handler  = h } ;
               duppy_return (close ())
             done
         | None ->  duppy_raise ()
      end ;
      write "\r\n\r\n"
    done
  with
    | _ -> duppy_return ()

let file_request path _ request = 
  let uri = 
    match request.request_uri with
      | "/" -> "/index.html"
      | s   -> s
  in
  let fname = 
    Printf.sprintf "%s%s" path uri
  in
  if Sys.file_exists fname then
    try
      let fd = Unix.openfile fname [Unix.O_RDONLY] 0o640 in
      let stats = Unix.fstat fd in
      let headers =
        [ "Date", date (Unix.gmtime (Unix.gettimeofday ()));
          "Server", server;
          "Content-Length", string_of_int (stats.Unix.st_size) ]
      in
      let headers = 
        if Pcre.pmatch ~rex:(Pcre.regexp "\\.html$") fname then
          ("Content-Type","text/html") :: headers
        else if Pcre.pmatch ~rex:(Pcre.regexp "\\.css$") fname then
          ("Content-Type","text/css") :: headers
        else
          headers
      in
      { reply_protocol = request.request_protocol ;
        reply_status   = (200,"OK") ;
        reply_headers  = headers ;
        reply_data     = File fd }
    with
      | _ -> error_403 request.request_protocol
  else
    error_404 request.request_protocol

let file_handler = 
  (fun _ -> true),file_request !files_path

let handlers = [file_handler]

let handle_request h request = 
  let f (check,handler) =
    if check request then
      duppy_raise (handler h request)
    else
      duppy_return ()
  in
  duppy_try
   duppy_do
     duppy_iter f handlers ;
     duppy_return (error_404 request.request_protocol)
   done
  with
    | reply -> duppy_return reply

let parse_headers headers =
  let split_header l h =
    try
      let rex = Pcre.regexp "([^:\\r\\n]+):\\s*([^\\r\\n]+)" in
      let sub = Pcre.exec ~rex h in
      duppy_return
        ((Pcre.get_substring sub 1,
          Pcre.get_substring sub 2) :: l)
    with
      | Not_found -> duppy_raise error_500
  in
  duppy_fold_left split_header [] headers

let parse_request h r = 
  try
    let headers = Pcre.split ~pat:"\r\n" r in
    duppy request,headers =
      match headers with
        | e :: l -> 
             duppy headers = 
               parse_headers l
             in 
             duppy_return (e,headers)
        | _ -> duppy_raise error_500
    in
    let rex = Pcre.regexp "([\\w]+)\\s([^\\s]+)\\s(HTTP/1.[01])" in
    duppy http_method,uri,protocol =
      try
        let sub = Pcre.exec ~rex request in
        let http_method,uri,protocol =
          Pcre.get_substring sub 1,
          Pcre.get_substring sub 2,
          Pcre.get_substring sub 3
        in
        duppy_return
          (method_of_string http_method,
           uri,
           protocol_of_string protocol)
      with
        | _ -> duppy_raise error_500
    in
    duppy data = 
      match http_method with
        | Get -> duppy_return None
        | Post ->
            duppy len =
             try
              let length = assoc_uppercase "CONTENT-LENGTH" headers in
              duppy_return (int_of_string length)
             with
               | Not_found -> duppy_return 0
               | _ -> duppy_raise error_500
            in 
            match len with
              | 0 -> duppy_return None
              | d -> 
                  duppy data = 
                    duppy_read
                      Duppy.Io.Length d
                    with
                      { priority = Non_blocking ;
                        handler  = h }
                  in
                  duppy_return (String data)
    in
    duppy_return
     { request_method   = http_method ;
       request_protocol = protocol ;
       request_uri      = uri ;
       request_headers  = headers ;
       request_data     = data } 
  with
    | _ -> duppy_raise error_500

let handle_client socket =
  let on_error e =
    match e with
      | Duppy.Io.Io_error -> ()
      | Duppy.Io.Unix (c,p,m) ->
          Printf.printf "%s" (Printexc.to_string
                               (Unix.Unix_error (c,p,m)))
      | Duppy.Io.Unknown e ->
          Printf.printf "%s" (Printexc.to_string e)
  in
  let h =
    { Duppy.Monad.Io.
       scheduler = scheduler ;
       socket    = socket ;
       init      = "";
       on_error  = on_error }
  in
  (* Read and process lines *)
  let rec exec () =
    duppy data =
       duppy_read
         Duppy.Io.Split "\r\n\r\n"
       with
         { priority = Non_blocking ;
           handler  = h }
    in
    duppy (do_close,reply) =
     let h =
       { h with
           Duppy.Monad.Io.
            on_error =
             (fun e ->
               on_error e;
               error_500) }
     in 
     duppy_try
       duppy request =
         parse_request h data
       in
       duppy reply = 
         handle_request h request
       in
       let close_header =
         try
           (assoc_uppercase "CONNECTION" request.request_headers) = "close"
         with
           | Not_found -> false
       in
       let do_close = 
         request.request_protocol = Http_10 ||
         close_header
       in
       duppy_return (do_close,reply)
     with
       | reply -> duppy_return (true,reply)
    in
    duppy_do
      send_reply h reply ;
      if do_close then
        duppy_raise ()
      else
        exec ()
    done
  in
  duppy_run 
    exec () 
  with
    | _ ->
      (try
        Unix.close socket
       with
         | _ -> ())

let new_queue ~priority ~name () =
   let priorities p = p = priority in
   let queue () =
      Duppy.queue scheduler ~log:(fun _ -> ()) ~priorities name
   in
   Thread.create queue ()

let bind_addr_inet = Unix.inet_addr_of_string "0.0.0.0"
let bind_addr = Unix.ADDR_INET(bind_addr_inet, !port)
let max_conn = 100
let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 
let () = 
  Unix.setsockopt sock Unix.SO_REUSEADDR true ;
  let rec incoming _ =
   begin
    try
      let (s,caller) = Unix.accept sock in
      handle_client s
    with e ->
       Printf.printf "Failed to accept new client: %S\n"
                (Printexc.to_string e)
   end ;
   [{ Duppy.Task.
       priority = Non_blocking ;
       events = [`Read sock] ;
       handler = incoming }]
  in
  begin 
   try 
    Unix.bind sock bind_addr 
   with
     | Unix.Unix_error(Unix.EADDRINUSE, "bind", "") ->
           failwith (Printf.sprintf "port %d already taken" !port)
  end ;
  Unix.listen sock max_conn ;
  Duppy.Task.add scheduler
   { Duppy.Task.
       priority = Non_blocking ;
       events   = [`Read sock] ;
       handler  = incoming } ;
  for i = 1 to !non_blocking_queues do
    ignore(new_queue ~priority:Non_blocking
             ~name:(Printf.sprintf "Non blocking queue #%d" i) 
             ())
  done ;
  for i = 1 to !maybe_blocking_queues do
    ignore(new_queue ~priority:Maybe_blocking
             ~name:(Printf.sprintf "Maybe blocking queue #%d" i)
             ()) 
  done ;
  Duppy.queue scheduler ~log:(fun _ -> ()) "root"
