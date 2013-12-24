(*****************************************************************************

  Duppy, a task scheduler for OCaml.
  Copyright 2003-2010 Savonet team

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details, fully stated in the COPYING
  file at the root of the liquidsoap distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

 *****************************************************************************)

type 'a scheduler = Mutex.t * Thread.t list ref

let create ?(compare=compare) () : 'a scheduler = Mutex.create (), ref []

module Task = 
struct
  type fd = Unix.file_descr

  (** Events and tasks from the user's point-of-view. *)
  type event = [
    | `Delay     of float
    | `Write     of fd
    | `Read      of fd
    | `Exception of fd
  ]
  
  type ('a,'b) task = {
    priority : 'a ;
    events   : 'b list ;
    handler  : 'b list -> ('a,'b) task list
  }

  let rec add (s : 'a scheduler) (t : ('a,[<event] as 'b) task) =
    let read = ref [] in
    let write = ref [] in
    let exc = ref [] in
    let timeout = ref (-1.) in
    List.iter (function
    | `Read fd -> read := fd :: !read
    | `Write fd -> write := fd :: !write
    | `Exception fd -> exc := fd :: !exc
    | `Delay d -> if !timeout < 0. then timeout := d else timeout := min !timeout d
    ) t.events;
    let m, threads = s in
    Mutex.lock m;
    let thread = Thread.create
      (fun () ->
        let read, write, exc = Unix.select !read !write !exc !timeout in
        let events =
          List.filter (function
          | `Read fd -> List.mem fd read
          | `Write fd -> List.mem fd write
          | `Exception fd -> List.mem fd exc
          | `Delay _ -> false
          ) t.events
        in
        let tt = t.handler events in
        List.iter (add s) tt;
        Mutex.lock m;
        let self = Thread.self () in
        threads := List.filter (fun t -> t <> self) !threads;
        Mutex.unlock m
      ) ()
    in
    threads := thread :: !threads;
    Mutex.unlock m
end

open Task

let queue ?log ?priorities s name =
  ()

let stop s =
  let m, threads = s in
  Mutex.lock m;
  List.iter Thread.kill !threads;
  threads := [];
  Mutex.unlock m

module Async =
struct
  (* m is used to make sure that 
   * calls to [wake_up] and [stop]
   * are thread-safe. *)
  type t = 
   { 
     stop       : bool ref;
     mutable fd : fd option;
     m          : Mutex.t
   }

  exception Stopped

  let add ~priority (scheduler:'a scheduler) f = 
   (* A pipe to wake up the task *)
   let out_pipe,in_pipe = Unix.pipe () in
   let stop = ref false in
   let tmp = String.create 1024 in
   let rec task l =
      if List.exists ((=) (`Read out_pipe)) l then
        (* Consume data from the pipe *)
        ignore (Unix.read out_pipe tmp 0 1024) ;
      if !stop then begin
        begin 
          try
            (* This interface is purely asynchronous
             * so we close both sides of the pipe here. *)
            Unix.close in_pipe ;
            Unix.close out_pipe 
          with _ -> ()
        end ;
        [] 
      end
      else begin
        let delay = f () in
        let event = 
          if delay >= 0. then
            [`Delay delay ]
          else
            []
        in
        [{ priority = priority ;
           events = `Read out_pipe :: event ;
           handler = task }]
      end
   in
   let task = 
     {
       priority = priority ;
       events = [`Read out_pipe] ;
       handler  = task
     }
   in
   add scheduler task ;
   { stop = stop ; fd = Some in_pipe ; 
     m = Mutex.create () }

  let wake_up t =
    Mutex.lock t.m ;
    try
     begin
      match t.fd with
         | Some t -> ignore (Unix.write t " " 0 1)
         | None -> raise Stopped
     end ;
     Mutex.unlock t.m
    with
      | e -> Mutex.unlock t.m; raise e
    

  let stop t = 
    Mutex.lock t.m;
    try 
      begin
       match t.fd with
         | Some c ->
              t.stop := true ; 
              ignore (Unix.write c " " 0 1)
         | None   -> raise Stopped
      end ;
      t.fd <- None ;
      Mutex.unlock t.m
    with
      | e -> Mutex.unlock t.m; raise e
end

module Io = 
struct
  type marker = Length of int | Split of string
  type failure = 
    | Io_error 
    | Unix of Unix.error*string*string 
    | Unknown of exn
    | Timeout

  exception Io
  exception Timeout_exc

  type bigarray = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  external ba_write : Unix.file_descr -> bigarray -> int -> int -> int = "ocaml_duppy_write_ba"

  let read ?(recursive=false) ?(init="") ?(on_error=fun _ -> ())
           ?timeout ~priority (scheduler:'a scheduler) 
           socket marker exec = 
    let length = 1024 in
    let b = Buffer.create length in
    let buf = String.make length ' ' in
    Buffer.add_string b init ;
    let events,check_timeout = 
      match timeout with
        | None -> [`Read socket], fun _ -> false
        | Some f -> [`Read socket; `Delay f],
                    (List.mem (`Delay f))
    in
    let rec f l =
      if check_timeout l then
        raise Timeout_exc ;
      if (List.mem (`Read socket) l) then
       begin
        let input = Unix.read socket buf 0 length in
        if input<=0 then raise Io ;
        Buffer.add_substring b buf 0 input
       end ;
      let ret = 
        match marker with
          | Split r ->
                let rex = Pcre.regexp r in
                let acc = Buffer.contents b in
                let ret = Pcre.full_split ~max:2 ~rex acc  in
                let rec p l = 
                 match l with
                   | Pcre.Text x :: Pcre.Delim _ :: l -> 
                       let f b x =
                         match x with
                           | Pcre.Text s 
                           | Pcre.Delim s -> Buffer.add_string b s 
                           | _      -> ()
                       in
                       if recursive then
                        begin
                         Buffer.reset b;
                         List.iter (f b) l ;
                         Some (x,None)
                        end
                       else
                        begin
                         let b = Buffer.create 10 in
                         List.iter (f b) l ;
                         Some (x, Some (Buffer.contents b))
                        end
                   | _ :: l' -> p l'
                   | [] -> None
                in
                p ret
          | Length n when n <= Buffer.length b -> 
              let s = Buffer.sub b 0 n in
              let rem = Buffer.sub b n (Buffer.length b - n) in
              if recursive then
               begin
                Buffer.reset b ;
                Buffer.add_string b rem ;
                Some (s, None)
               end
              else
                Some (s, Some rem)
          | _ -> None
      in
      (* Catch all exceptions.. *)
      let f x =
       try
        f x
       with
         | Io -> on_error (Buffer.contents b,Io_error); []
         | Timeout_exc -> on_error (Buffer.contents b,Timeout); []
         | Unix.Unix_error(x,y,z) -> 
                 on_error (Buffer.contents b,Unix(x,y,z)); []
         | e ->  on_error (Buffer.contents b,Unknown e); []
      in
      match ret with
        | Some x ->
            begin
              match x with
                | s,Some s' when recursive ->
                     exec (s,None) ;
                     [{ priority = priority ; 
                        events = events ;
                        handler = f }]
                | _ -> exec x; []
            end
        | None ->
             [{ priority = priority ; 
                events = events ;
                handler = f }]
    in
    (* Catch all exceptions.. *)
    let f x = 
      try
        f x 
      with
        | Io -> on_error (Buffer.contents b,Io_error); []
        | Timeout_exc -> on_error (Buffer.contents b,Timeout); []
        | Unix.Unix_error(x,y,z) -> 
                on_error (Buffer.contents b,Unix(x,y,z)); []
        | e ->  on_error (Buffer.contents b,Unknown e); []
    in
      (* First one is without read,
       * in case init contains the wanted match. 
       * Unless the user sets timeout to 0., this
       * should not interfer with user-defined timeout.. *)
      let task = 
       {
         priority = priority ;
         events = [`Delay 0.; `Read socket] ;
         handler  = f
       }
       in
      add scheduler task

  let write ?(exec=fun () -> ()) ?(on_error=fun _ -> ()) 
            ?bigarray ?string ?timeout ~priority
            (scheduler:'a scheduler) socket = 
    let length,write = 
      match string,bigarray with
        | Some s,_ -> 
            String.length s,
            Unix.write socket s
        | None,Some b ->
            Bigarray.Array1.dim b,
            ba_write socket b
        | _ ->
            0,fun _ _ -> 0
    in
    let events,check_timeout =
      match timeout with
        | None -> [`Write socket], fun _ -> false
        | Some f -> [`Write socket; `Delay f],
                    (List.mem (`Delay f))
    in
    let rec f pos l =
     try
      if check_timeout l then
       raise Timeout_exc ;
      assert (List.exists ((=) (`Write socket)) l) ;
      let len = length - pos in
      let n = write pos len in
      if n<=0 then (on_error Io_error ; [])
      else
      begin
        if n < len then
          [{ priority = priority ; events = [`Write socket] ;
             handler = f (pos+n) }]
        else
          (exec () ; [])
      end
     with
       | Timeout_exc -> on_error Timeout; []
       | Unix.Unix_error(x,y,z) -> on_error (Unix(x,y,z)); []
       | e -> on_error (Unknown e); []
    in  
    if length > 0 then
        let task = 
          {
            priority = priority ;
            events = events ;
            handler  = (f 0)
          }
        in
        add scheduler task
    else 
        exec ()
end

(** A monad for implicit
  * continuations or responses *)
module Monad = 
struct
  type ('a,'b) handler = 
    { return: 'a -> unit ;
      raise:  'b -> unit }
  type ('a,'b) t  = ('a,'b) handler -> unit

  let return (x:'a) : ('a,'b) t =
    fun h -> h.return x 

  let raise x = 
    fun h -> h.raise x

  let bind f g =
    fun h ->
      let ret x = 
        let process = g x in
        process h
      in
      f { return = ret ;
          raise  = h.raise }

  let (>>=) = bind

  let run ~return:ret ~raise:raise f = 
    f { return = ret ;
        raise  = raise }

  let catch f g = 
    fun h ->
      let raise x = 
        let process = g x in
        process h
     in
     f { return = h.return ;
         raise  = raise }

  let (=<<) = fun x y -> catch y x

  let rec fold_left f a =
    function
       | [] -> a
       | b :: l ->
          fold_left f (bind a (fun a -> f a b)) l

  let fold_left f a l = fold_left f (return a) l

  let iter f l = fold_left (fun () b -> f b) () l

  module Mutex_o = Mutex

  module Mutex = 
  struct
    module type Mutex_control =
    sig
      type priority
      val scheduler : priority scheduler
      val priority  : priority 
    end

    module type Mutex_t =
    sig
      (** Type for a mutex. *)
      type mutex = Mutex.t

      module Control : Mutex_control

      (** [create ()] creates a mutex. Implementation-wise,
        * a duppy task is created that will be used to select a
        * waiting computation, lock the mutex on it and resume it.
        * Thus, [priority] and [s] represents, resp., the priority
        * and scheduler used when running calling process' computation. *)
      val create : unit -> mutex

      (** A computation that locks a mutex
        * and returns [unit] afterwards. Computation
        * will be blocked until the mutex is sucessfuly locked. *)
      val lock : mutex -> (unit,'a) t

      (** A computation that tries to lock a mutex.
        * Returns immediatly [true] if the mutex was sucesfully locked
        * or [false] otherwise. *)
      val try_lock : mutex -> (bool,'a) t

      (** A computation that unlocks a mutex.
        * Should return immediatly. *)
      val unlock : mutex -> (unit,'a) t
    end

    module Factory(Control:Mutex_control) = 
    struct
      module Control = Control

      type mutex = Mutex.t
     
      let create () = Mutex.create ()
      
      let lock m = return (Mutex.lock m)
      
      let try_lock m = return (Mutex.try_lock m)
      
      let unlock m = return (Mutex.unlock m)
    end
  end
  module Condition = 
  struct
    module Factory(Mutex : Mutex.Mutex_t) = 
    struct
      type condition = Condition.t

      module Control = Mutex.Control

      let create () = Condition.create ()

      (* Mutex.unlock m needs to happen _after_
       * the task has been registered. *)
      let wait c m = return (Condition.wait c m)

      let signal c = return (Condition.signal c)

      let broadcast c = return (Condition.broadcast c)
    end
  end

  module Io =
  struct
    type ('a,'b) handler = 
      { scheduler    : 'a scheduler ;
        socket       : Unix.file_descr ;
        mutable data : string ;
        on_error     : Io.failure -> 'b }
 
    let rec exec ?(delay=0.) ~priority h f = 
      (fun h' -> 
        let handler _ =
          begin
           try
             f h'
           with
             | e -> h'.raise (h.on_error (Io.Unknown e))
          end ;
          []
        in
        Task.add h.scheduler 
          { Task.
             priority = priority ;
             events   = [`Delay delay];
             handler  = handler })

    let delay ~priority h delay = 
      exec ~delay ~priority h (return ())

    let read ?timeout ~priority ~marker h = 
      (fun h' -> 
         let process x =
           let s = 
             match x with
               | s, None -> 
                   h.data <- "" ;
                   s
               | s, Some s' -> 
                   h.data <- s' ;
                   s
           in 
           h'.return s
         in
         let init = h.data in
         h.data <- "" ;
         let on_error (s,x) =
            h.data <- s ; 
            h'.raise (h.on_error x)
         in
         Io.read ?timeout ~priority ~init ~recursive:false 
                 ~on_error h.scheduler h.socket 
                 marker process)

   let read_all ?timeout ~priority s sock = 
     let handler =
          { scheduler = s ;
            socket = sock ;
            data = "" ;
            on_error = (fun e -> e) }
     in
     let buf = Buffer.create 1024 in
     let rec f () =
       let data =
         read ?timeout ~priority 
              ~marker:(Io.Length 1024)
              handler
       in
       let process data = 
         Buffer.add_string buf data ;
         f ()
        in
        data >>= process
     in
     let catch_ret e =
       Buffer.add_string buf handler.data ;
       match e with
           | Io.Io_error -> return (Buffer.contents buf)
           | e -> raise (Buffer.contents buf,e)
     in
     catch (f ()) catch_ret

    let write ?timeout ~priority h s =
      (fun h' ->
         let on_error x =
           h'.raise (h.on_error x)
         in
         let exec () = 
           h'.return ()
         in
         Io.write ?timeout ~priority ~on_error ~exec 
                  ~string:s h.scheduler h.socket)

    let write_bigarray ?timeout ~priority h ba =
      (fun h' ->
         let on_error x =
           h'.raise (h.on_error x)
         in
         let exec () =
           h'.return ()
         in
         Io.write ?timeout ~priority ~on_error ~exec
                  ~bigarray:ba h.scheduler h.socket)
  end
end

