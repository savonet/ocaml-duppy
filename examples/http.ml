
let duppy = Duppy.create ()
let priority = 0

let http_page code status =
  ( "HTTP/1.0 " ^ (string_of_int code) ^ " " ^ status ^ "\r\n" ^
    "Content-Type: text/html\r\n\r\n" )

let http_error_page code status msg =
  ( http_page code status ^
    "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" ^
    "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" " ^
    "\"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\n" ^
    "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">" ^
    "<head><title>Duppy httpd test example</title></head>" ^
    "<body><p>" ^ msg ^ "</p></body></html>\n" )

let serve e = 
  try
    let headers = Pcre.split ~pat:"\r\n" e in
    List.iter (Printf.printf "bla: %s\n") headers ;
    let request = 
      match headers with
        | e :: _ -> e
        | _ -> raise Not_found
    in
    let rex = Pcre.regexp "GET /([^\\s]+) HTTP/1.[01]" in
    let uri = 
        let sub = Pcre.exec ~rex request in
        Pcre.get_substring sub 1
    in
    Printf.printf "uri: %s\n" uri ;
    let f = open_in uri in
    let rec aux () =
      match
       try Some (input_line f) with End_of_file -> None
      with
       | None -> ""
       | Some s -> s ^ "\r\n" ^ (aux ())
    in
    http_page 200 "OK" ^ (aux ())
  with
    | _ -> http_error_page 404 "Not found" "The requested page could not be found"


let handle_client socket = 
  (* Read and process lines *)
  let marker = Duppy.Io.Split "\r\n\r\n" in
  let recursive = false in
  (* Process the command [s] and start a new reading poll *)
  let rec process l = 
    let l,init = 
      match List.rev l with
        | []
        | _ :: [] -> assert false (* Should not happen *)
        | e :: l -> List.rev l,e
    in
              List.iter (Printf.printf "string: '%s'\n") l; flush_all () ;
    let e = 
      match l with
        | e :: [] -> e
        | _ -> assert false
    in
    let answer = serve e in
    let exec () = 
      try
        Unix.close socket
      with
        | _ -> ()
    in
    Duppy.Io.write ~priority ~exec duppy socket answer
  in
  Duppy.Io.read ~priority ~recursive duppy socket marker process

open Unix

let port = 4123
let bind_addr_inet = inet_addr_of_string "0.0.0.0"
let bind_addr = ADDR_INET(bind_addr_inet, port)
let max_conn = 10 
let sock = socket PF_INET SOCK_STREAM 0 
let () = setsockopt sock SO_REUSEADDR true ;
         let rec incoming _ =
           begin
             try
               let (s,caller) = accept sock in
               let ip =
               let a = match caller with 
                   | ADDR_INET (a,_) -> a
                   | _ -> assert false
               in
              try
                (gethostbyaddr a).h_name
              with
                | Not_found -> string_of_inet_addr a
              in
              Printf.printf "New client: %s\n" ip ;
              handle_client s ;
            with e ->
              Printf.printf "Failed to accept new client: %S\n"
                (Printexc.to_string e)
           end ;
           [{ Duppy.Task.priority = priority ; 
	      Duppy.Task.events = [`Read sock] ;
              Duppy.Task.handler = incoming }]
         in
         begin try bind sock bind_addr with
           | Unix.Unix_error(Unix.EADDRINUSE, "bind", "") ->
              failwith (Printf.sprintf "port %d already taken" port)
         end ;
         listen sock max_conn ;
         Duppy.Task.add duppy 
	   { Duppy.Task.priority = priority ;
	     Duppy.Task.events   = [`Read sock] ;
	     Duppy.Task.handler  = incoming } ;
         Duppy.queue duppy ~log:(Printf.printf "%s\n%!") "root"
