

let duppy = Duppy.create ()
let priority = 0
exception Exit

let exec_command s () = 
  let chan = Unix.open_process_in s in
  let rec aux () =
    match
     try Some (input_line chan) with End_of_file -> None
    with
     | None -> []
     | Some s -> s::(aux ())
  in
  let l = aux () in
  ignore (Unix.close_process_in chan) ;
  String.concat "\r\n" l


let commands = Hashtbl.create 10
let () = Hashtbl.add commands "hello" (fun () -> "world") ;
         Hashtbl.add commands "foo" (fun () -> "bar") ;
         Hashtbl.add commands "uptime" (exec_command "uptime") ;
         Hashtbl.add commands "date" (exec_command "date") ;
         Hashtbl.add commands "whoami" (exec_command "whoami") ;
	 Hashtbl.add commands "exit" (fun () -> raise Exit) 
(* Add commands here *)
let help = ref "List of commands:" 
let () = Hashtbl.iter 
           (fun x _ -> help := Printf.sprintf "%s\n%s" !help x)
	   commands ;
	 Hashtbl.add commands "help" (fun () -> !help)


let handle_client socket = 
  (* Read and process lines *)
  let marker = Duppy.Io.Split "[\r\n]+" in
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
    let process_elem s = 
      let answer =
        try (Hashtbl.find commands s) () with
          | Not_found ->
              "ERROR: unknown command, type \"help\" to get a list of commands."
      in
      let len = String.length answer in
        if len > 0 && answer.[len - 1] = '\n' then
          answer^"END\r\n"
        else
          answer^"\r\nEND\r\n"
    in
    let l = List.map process_elem l in
    let answer = String.concat "\r\n" l in
    let exec () =
      Duppy.Io.read ~priority ~recursive ~init duppy socket marker process
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

let () =
  setsockopt sock SO_REUSEADDR true ;
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
        Printf.printf "Failed to accept new client: %S\n" (Printexc.to_string e)
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
    Printf.printf "Starting one queue on the scheduler...\n%!" ;
    Duppy.queue duppy ~log:(Printf.printf "%s\n%!") "root"
