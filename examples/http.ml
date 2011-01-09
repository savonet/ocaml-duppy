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
                       "Server", server ] ;
    reply_data     = data } 

let error_404 = 
  server_error (404,"File Not Found")

let error_500 =
   server_error (500,"Bad Request") Http_10

let error_403 = 
  server_error (403,"Forbidden")

let http_302 protocol uri = 
  { reply_protocol = protocol ;
    reply_status   = (302,"Found") ;
    reply_headers  = ["Location",uri];
    reply_data     = String "" }

type socket_status = Keep | Close

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
       | None -> duppy_return ()
    end 
    done

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

let index_uri path index protocol uri =
  let uri =
   try
     let ret =
        Pcre.extract ~rex:(Pcre.regexp "([^\\?]*)\\?")
                     uri
      in
      ret.(1)
   with
     | Not_found -> uri
  in
  try
   if Sys.is_directory 
        (Printf.sprintf "%s%s" path uri) 
   then
     if uri.[String.length uri - 1] <> '/' then
       duppy_raise (http_302 protocol (Printf.sprintf "%s/" uri)) 
     else
      begin
       let index = Printf.sprintf "%s/%s" uri index in
       if Sys.file_exists
            (Printf.sprintf "%s/%s" path index) then
         duppy_return index
       else
         duppy_return uri
      end
   else
      duppy_return uri
  with
    | _ -> duppy_return uri

let file_request path _ request = 
  let uri = 
    try
      let ret =
        Pcre.extract ~rex:(Pcre.regexp "([^\\?]*)\\?.*")
                     request.request_uri
      in
      ret.(1)
    with
      | Not_found -> request.request_uri
  in
  duppy uri = 
    index_uri path "index.html" 
                   request.request_protocol uri 
  in
  let fname = 
    Printf.sprintf "%s%s" path uri
  in
  if Sys.file_exists fname then
    try
      let fd = Unix.openfile fname [Unix.O_RDONLY] 0o640 in
      let stats = Unix.fstat fd in
      let headers =
        [ "Server", server;
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
      duppy_raise
       { reply_protocol = request.request_protocol ;
         reply_status   = (200,"OK") ;
         reply_headers  = headers ;
         reply_data     = File fd }
    with
      | _ -> duppy_raise (error_403 request.request_protocol)
  else
    duppy_raise (error_404 request.request_protocol)

let file_handler = 
  (fun _ -> duppy_return true),file_request !files_path

let cgi_handler process path h request =
  let uri,args,suffix = 
    try
      let ret = 
        Pcre.extract ~rex:(Pcre.regexp "([^\\?]*)\\?(.*)")
                     request.request_uri 
      in
      begin
       try
        let ans = 
          Pcre.extract ~rex:(Pcre.regexp "^([^/]*)/([^&=]*)$")
                       ret.(2)
        in
        ret.(1),ans.(1),ans.(2)
       with 
         | Not_found -> ret.(1),ret.(2),""
      end
    with
      | Not_found -> request.request_uri,"",""
  in
  duppy script = 
    index_uri path "index.php" 
              request.request_protocol
              uri 
  in
  let script = 
    Printf.sprintf "%s%s" path script
  in
  let env = 
    Printf.sprintf
        "export SERVER_SOFTWARE=Duppy-httpd/1.0; \
         export SERVER_NAME=localhost; \
         export GATEWAY_INTERFACE=CGI/1.1; \
         export SERVER_PROTOCOL=%s; \
         export SERVER_PORT=%d; \
         export REQUEST_METHOD=%s; \
         export REQUEST_URI=%s; \
         export REDIRECT_STATUS=200; \
         export SCRIPT_FILENAME=%s"
       (string_of_protocol (request.request_protocol))
       !port
       (string_of_method (request.request_method))
       (Filename.quote uri)
       (Filename.quote script)
  in
  let env = 
    Printf.sprintf "%s; export QUERY_STRING=%s"
                   env
                   (Filename.quote args)
  in
  let env = 
    let tr_suffix = 
      Printf.sprintf "%s%s" path suffix 
    in
    (* Trick ! *)
    let tr_suffix = 
      Printf.sprintf "%s/%s" (Filename.dirname tr_suffix)
                             (Filename.basename tr_suffix)
    in
    Printf.sprintf "%s; export PATH_TRANSLATED=%s; \
                        export PATH_INFO=%s"
                   env
                   (Filename.quote tr_suffix)
                   (Filename.quote suffix)
  in
  let sanitize s =
    Pcre.replace ~pat:"-" ~templ:"_" (String.uppercase s)
  in
  let headers = 
    List.map (fun (x,y) -> (sanitize x,y)) request.request_headers
  in
  let append env key = 
    if List.mem_assoc key headers then
      Printf.sprintf "%s; export %s=%s"
        env key (Filename.quote (List.assoc key headers))
    else
      env
  in
  let env = append env "CONTENT_TYPE" in
  let env = append env "CONTENT_LENGTH" in
  duppy env = 
    if List.mem_assoc "AUTHORIZATION" headers then
     begin
      let ret = 
        Pcre.extract ~rex:(Pcre.regexp "(^[^\\s]*\\s.*)$") 
                     (List.assoc "AUTHORIZATION" headers)
      in
      if Array.length ret > 0 then
        duppy_return (Printf.sprintf "%s; extract AUTH_TYPE=%s" env (ret.(1)))
      else
        duppy_raise error_500 
     end
    else
      duppy_return env
  in
  let f env (x,y) = 
    Printf.sprintf "%s; export HTTP_%s=%s"
      env x (Filename.quote y)
  in
  let env = List.fold_left f env headers in
  let data = 
    match request.request_data with
      | None -> ""
      | String s -> s
      | _ -> assert false (* not implemented *)
  in
  let process = 
    Printf.sprintf "%s; %s 2>/dev/null"
      env process
  in
  let in_c,out_c = 
    Unix.open_process process
  in
  let out_s = 
    Unix.descr_of_out_channel out_c
  in
  let h =
    { h with
       Duppy.Monad.Io.
         socket = out_s ;
         data   = ""
    }
  in
  duppy () = 
    duppy_write
      data
    with
      { priority = Non_blocking ;
        handler  = h }
  in
  let in_s = 
    Unix.descr_of_in_channel in_c 
  in
  let h = 
    { h with
       Duppy.Monad.Io.
         socket = in_s ;
         data   = ""
    }
  in
  duppy headers = 
    duppy_read
      Duppy.Io.Split "[\r]?\n[\r]?\n"
    with
      { priority = Non_blocking ;
        handler  = h }
  in
  duppy data = 
    duppy_try
     duppy_read_all
       in_s
     with
       { priority = Non_blocking ;
         scheduler = h.Duppy.Monad.Io.scheduler }
    with
      | (s,_) -> duppy_return s
  in
  let data = 
    Printf.sprintf "%s%s" h.Duppy.Monad.Io.data data 
  in
  ignore(Unix.close_process (in_c,out_c)) ;
  duppy headers = 
    let headers = Pcre.split ~pat:"\r\n" headers in
    parse_headers headers 
  in
  duppy status,headers = 
    if List.mem_assoc "Status" headers then
     try
      let ans = Pcre.extract ~rex:(Pcre.regexp "([\\d]+)\\s(.*)")
                             (List.assoc "Status" headers) 
      in
      duppy_return
       ((int_of_string ans.(1),
         ans.(2)),
        List.filter (fun (x,y) -> x <> "Status") headers)
     with _ -> duppy_raise error_500
    else duppy_return ((200,"OK"),headers)
  in
  let headers = 
    ("Content-length",string_of_int (String.length data))::
      headers
  in
  duppy_raise
    { reply_protocol = request.request_protocol ;
      reply_status   = status ;
      reply_headers  = headers ;
      reply_data     = String data }

let php_handler = 
  (fun request -> 
     duppy uri = 
       index_uri !files_path "index.php" 
                 request.request_protocol
                 request.request_uri 
     in
     duppy_return (Pcre.pmatch ~rex:(Pcre.regexp "\\.php$") uri)),
  cgi_handler "php-cgi" !files_path

let handlers = [php_handler;file_handler]

let handle_request h request = 
  let f (check,handler) =
    duppy check = check request in
    if check then
      handler h request
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
  (* Read and process lines *)
  let on_error e =
    error_500
  in
  let h =
    { Duppy.Monad.Io.
       scheduler = scheduler ;
       socket    = socket ;
       data      = "";
       on_error  = on_error }
  in
  let rec exec () =
    duppy (keep,reply) = 
      duppy_try
        duppy data =
          duppy_read
            Duppy.Io.Split "\r\n\r\n"
          with
            { priority = Non_blocking ;
              handler  = h }
        in
        duppy request =
          parse_request h data
        in
        duppy reply = 
          handle_request h request
        in
        let close_header headers =
          try
            (assoc_uppercase "CONNECTION" headers) = "close"
          with
            | Not_found -> false
        in
        let keep = 
          if
            request.request_protocol = Http_10 ||
            close_header request.request_headers ||
            close_header reply.reply_headers
          then
            Close
          else
            Keep
        in
        duppy_return (keep,reply)
      with
        | reply -> duppy_return (Close,reply)
    in
    duppy_do
        send_reply h reply ;
        if keep = Keep then
          exec ()
        else
          duppy_return ()
    done
  in
  let finish _ = 
    try
      Unix.close socket
    with
      | _ -> ()
  in 
  duppy_run 
    exec ()
  with
    { return = finish ;
      raise = finish }

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
  (* See http://caml.inria.fr/mantis/print_bug_page.php?bug_id=4640
   * for this: we want Unix EPIPE error and not SIGPIPE, which
   * crashes the program.. *)
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  ignore (Unix.sigprocmask Unix.SIG_BLOCK [Sys.sigpipe]);
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
