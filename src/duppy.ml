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

type fd = Unix.file_descr

(** [remove f l] is like [List.find f l] but also returns the result of removing
  * the found element from the original list. *)
let remove f l =
  let rec aux acc = function
    | [] -> raise Not_found
    | x::l -> if f x then x, List.rev_append acc l else aux (x::acc) l
  in
    aux [] l

(** Events and tasks from the implementation point-of-view:
  * we have to hide the 'a parameter. *)

type e = {
  r : fd list ;
  w : fd list ;
  x : fd list ;
  t : float
}

type 'a t = {
  timestamp : float ;
  prio : 'a ;
  enrich   : e -> e ;
  is_ready : e -> (unit -> 'a t list) option
}

type 'a scheduler =
{
  out_pipe : Unix.file_descr;
  in_pipe : Unix.file_descr;
  compare : 'a -> 'a -> int;
  select_m : Mutex.t;
  mutable tasks : 'a t list;
  tasks_m : Mutex.t;
  mutable ready : ('a * (unit -> 'a t list)) list;
  ready_m : Mutex.t;
  mutable queues : Condition.t list;
  queues_m : Mutex.t;
  mutable stop : bool;
}

let clear_tasks s = 
  Mutex.lock s.tasks_m ;
  s.tasks <- [] ;
  Mutex.unlock s.tasks_m 

let create ?(compare=compare) () = 
  let out_pipe,in_pipe = Unix.pipe () in
  {
    out_pipe = out_pipe;
    in_pipe = in_pipe;
    compare = compare;
    select_m = Mutex.create ();
    tasks = [];
    tasks_m = Mutex.create ();
    ready = [];
    ready_m = Mutex.create ();
    queues = [];
    queues_m = Mutex.create ();
    stop = false;
  }

let wake_up s = ignore (Unix.write s.in_pipe "x" 0 1)

module Task = 
struct
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
  let time () = Unix.gettimeofday ()
  
  let rec t_of_task (task:('a,[<event])task) =
    (** Test task sockets. There's no easy way to test
      * sockets passed to [select] before hand for potential
      * exception raised there. Once [select] raises an
      * exception with multiple sockets passed, it is impossible
      * to know which socket caused that exception, leading to
      * fatal errors. Thus, we test each socket individually here
      * using a [select] to stick to actual conditions. Please
      * note that there is still a chance that the socket is closed
      * between this test and the time it is actually pushed down 
      * to the main [select] call. *)
    let prepare = function
      | `Read socket  ->
          ignore(Unix.select [socket] [] [] 0.)
      | `Write socket ->
          ignore(Unix.select [] [socket] [] 0.)
      | `Exception socket ->
          ignore(Unix.select [] [] [socket] 0.)
      | `Delay  _ -> ()
    in
    List.iter prepare task.events;
    let t0 = time () in
      { timestamp = t0 ; prio = task.priority ;
        enrich = (fun e ->
                    List.fold_left
                      (fun e -> function
                         | `Delay     s -> { e with t = min e.t (t0+.s) }
                         | `Read      s -> { e with r = s::e.r }
                         | `Write     s -> { e with w = s::e.w }
                         | `Exception s -> { e with x = s::e.x })
                      e task.events) ;
        is_ready = (fun e ->
                      let l =
                        List.filter
                          (fun evt ->
                             match (evt :> event) with
                               | `Delay     s when time () > t0+.s -> true
                               | `Read      s when List.mem s e.r -> true
                               | `Write     s when List.mem s e.w -> true
                               | `Exception s when List.mem s e.x -> true
                               | _ -> false)
                          task.events
                      in
                        if l = [] then None else
                          Some (fun () -> List.map t_of_task (task.handler l)))
      }
  
  let add_t s items =
    let f item = 
      match item.is_ready {r=[];w=[];x=[];t=0.} with
        | Some f -> 
            Mutex.lock s.ready_m ;
            s.ready <- (item.prio,f) :: s.ready ;
            Mutex.unlock s.ready_m
        | None ->
            Mutex.lock s.tasks_m ;
            s.tasks <- item :: s.tasks ;
            Mutex.unlock s.tasks_m ;
    in
    List.iter f items ;
    wake_up s
  
  let add s t  = add_t s [t_of_task t]
end

open Task

let stop s = 
  s.stop <- true ;
  wake_up s

let tmp = String.create 1024

(** There should be only one call of #process at a time.
  * Process waits for tasks to become ready, and moves ready tasks
  * to the ready queue. *)
let process s log = 
  (* Compute the union of all events. *)
  let e =
    List.fold_left
      (fun e t -> t.enrich e)
      { r = [s.out_pipe] ; w = [] ; x = [] ; t = infinity }
      s.tasks
  in
  (* Poll for an event. *)
  let r,w,x =
    let rec f () = 
      try
        let timeout = 
          if e.t = infinity then -1. else max 0. (e.t -. (time ())) 
        in
        log (Printf.sprintf "Enter select at %f, timeout %f (%d/%d/%d)."
             (time ()) timeout
           (List.length e.r) (List.length e.w) (List.length e.x)) ;
        let r,w,x = Unix.select e.r e.w e.x timeout in
        log (Printf.sprintf "Left select at %f (%d/%d/%d)." (time ())
               (List.length r) (List.length w) (List.length x)) ;
        r,w,x
      with
        | Unix.Unix_error (Unix.EINTR,_,_) ->
          (* [EINTR] means that select was interrupted by 
           * a signal before any of the selected events 
           * occurred and before the timeout interval expired.
           * We catch it and restart.. *) 
           log (Printf.sprintf "Select interrupted at %f." (time ())) ;
           f ()
        | e -> 
           (* Uncaught exception: 
            * 1) Discards all tasks currently in the loop (we do not know which 
            *    socket caused an error).
            * 2) Re-Raise e *)
           clear_tasks s ; 
           raise e 
    in
    f ()
  in
  (* Empty the wake_up pipe if needed. *)
  let () =
    if List.mem s.out_pipe r then
      (* For safety, we may absorb more than
       * one write. This avoids bad situation
       * when exceesive wake_up may fill up the
       * pipe's write buffer, causing a wake_up 
       * to become blocking..  *)
      ignore (Unix.read s.out_pipe tmp 0 1024)
  in
  (* Move ready tasks to the ready list. *)
  let e = { r=r ; w=w ; x=x ; t=0. } in
    Mutex.lock s.tasks_m ;
    (* Split [tasks] into [r]eady and still [w]aiting. *)
    let r,w =
      List.fold_left
        (fun (r,w) t ->
           match t.is_ready e with
             | Some f -> (t.prio,f)::r, w
             | None -> r, t::w)
        ([],[])
        s.tasks
    in
      s.tasks <- w ;
      Mutex.unlock s.tasks_m ;
      Mutex.lock s.ready_m ;
      s.ready <- List.stable_sort (fun (p,_) (p',_) -> s.compare p p') (s.ready @ r) ;
      Mutex.unlock s.ready_m

  (** Code for a queue to process ready tasks.
    * Returns true a task was found (and hence processed).
    *
    * s.ready_m *must* be locked before calling
    * this function, and is freed *only* 
    * if some task was processed. *) 
let exec s (priorities:'a->bool) =
  (* This assertion does not work on
   * win32 because a thread can double-lock
   * the same mutex.. *)
  if Sys.os_type <> "Win32" then
    assert(not (Mutex.try_lock s.ready_m)) ;
  try
    let (_,task),remaining =
      remove
        (fun (p,f) ->
           priorities p)
        s.ready
    in
      s.ready <- remaining ;
      Mutex.unlock s.ready_m ;
      add_t s (task ()) ;
      true
  with Not_found -> false

 (** Main loop for queues. *)
let queue ?log ?(priorities=fun _ -> true) s name = 
  let log = 
    match log with
      | Some e -> e
      | None -> Printf.printf "queue %s: %s\n" name
  in
  let c =
    let c = Condition.create () in
      Mutex.lock s.queues_m ;
      s.queues <- c::s.queues ;
      Mutex.unlock s.queues_m ;
      log (Printf.sprintf "Queue #%d starting..." (List.length s.queues)) ;
      c
  in
    (* Try to process ready tasks, otherwise try to become the master,
     * or be a slave and wait for the master to get some more ready tasks. *)
    while not s.stop do
      (* Lock the ready tasks until the queue has a task to proceed,
       * *or* is really ready to restart on its condition, see the 
       * Condition.wait call below for the atomic unlock and wait. *)
      Mutex.lock s.ready_m ;
      log (Printf.sprintf "There are %d ready tasks." (List.length s.ready)) ;
      if exec s priorities then () else 
          let wake () = 
            (* Wake up other queues 
	     * if there are remaining tasks *)
            if s.ready <> [] then begin
	        Mutex.lock s.queues_m ;
                List.iter (fun x -> if x <> c then Condition.signal x) 
		          s.queues ;
	        Mutex.unlock s.queues_m 
	      end ;
          in
          if Mutex.try_lock s.select_m then begin
	    (* Processing finished for me
	     * I can unlock ready_m now.. *)
	    Mutex.unlock s.ready_m ;
            process s log ;
            Mutex.unlock s.select_m ;
	    Mutex.lock s.ready_m ;
            wake () ;
	    Mutex.unlock s.ready_m
          end else begin
              (* We use s.ready_m mutex here.
	       * Hence, we avoid race conditions
	       * with any other queue being processing
	       * a task that would create a new task: 
	       * without this mutex, the new task may not be 
	       * notified to this queue if it is going to sleep
	       * in concurrency..
	       * It also avoid race conditions when restarting 
	       * queues since s.ready_m is locked until all 
	       * queues have been signaled. *)
              Condition.wait c s.ready_m;
              Mutex.unlock s.ready_m
            end
    done

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

  let return x = 
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
      type mutex

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
      (* A mutex is either locked or not
       * and has a list of tasks waiting to get
       * it. *)
      type mutex =
        { mutable locked : bool ;
          mutable tasks  : ((unit->unit) list) }

      module Control = Control

      let tmp = String.create 1024

      let (x,y) = Unix.pipe ()
      
      let stop = ref false
      
      let wake_up () = ignore(Unix.write y " " 0 1)
      
      let ctl_m = Mutex_o.create ()
     
      let finalise _ = 
        stop := true ;
        wake_up ()
 
      let mutexes = Queue.create ()

      let () = Gc.finalise finalise mutexes
     
      let register () = 
        let m = 
          { locked = false ;
            tasks = [] } 
        in
        Queue.push m mutexes;
        m

      let cleanup m =
        Mutex_o.lock ctl_m ;
        let q = Queue.create () in
        Queue.iter (fun m' -> if m <> m' then Queue.push m q) mutexes ;
        Queue.clear mutexes ;
        Queue.transfer q mutexes ;
        Mutex_o.unlock ctl_m
      
      let task f =
         { Task.
           priority = Control.priority ;
           events = [`Delay 0.];
           handler = (fun _ -> f (); [])}
      
      (* This should only be called when [ctl_m] is locked. *)
      let process_mutex tasks m =
        if not m.locked then
          (* I don't think shuffling tasks
           * matters here.. *)
          match m.tasks with
            | x :: l ->
               m.tasks <- l ;
               m.locked <- true ;
               task x :: tasks
            | _ -> tasks
        else tasks
      
      let rec handler _ = 
         Mutex_o.lock ctl_m ;
         if not !stop then 
          begin
           let tasks = 
             Queue.fold process_mutex [] mutexes
           in
           Mutex_o.unlock ctl_m ;
           ignore(Unix.read x tmp 0 1024) ;
            { Task.
               priority = Control.priority ;
               events   = [`Read x];
               handler  = handler }  :: tasks
          end
         else
          begin
           Mutex_o.unlock ctl_m ;
           try
             Unix.close x;
             Unix.close y;
             []
           with
             | _ -> 
                []
          end
      
      let () = 
        Task.add Control.scheduler
         { Task.
            priority = Control.priority;
            events   = [`Read x];
            handler  = handler } 
     
      let create () = 
        Mutex_o.lock ctl_m ;
        let ret = register () in
        Mutex_o.unlock ctl_m ;
        Gc.finalise cleanup ret ;
        ret
      
      let lock m =
        (fun h' ->
          Mutex_o.lock ctl_m ;
          if not m.locked then
           begin
            m.locked <- true ;
            Mutex_o.unlock ctl_m ;
            h'.return ()
           end
          else
           begin
            m.tasks <- h'.return :: m.tasks ;
            Mutex_o.unlock ctl_m
           end)
      
      let try_lock m = 
        (fun h' ->
           Mutex_o.lock ctl_m ;
           if not m.locked then
            begin
             m.locked <- true ;
             Mutex_o.unlock ctl_m ;
             h'.return true ;
            end
           else
            begin
             Mutex_o.unlock ctl_m ;
             h'.return false
            end)
      
      let unlock m =
        (fun h' -> 
          Mutex_o.lock ctl_m ;
          (* Here we allow inter-thread 
           * and double unlock.. Double unlock
           * is not necessarily a problem and
           * inter-thread unlock well.. what is
           * a thread here ?? :-) *)
          m.locked <- false ;
          let wake = m.tasks <> [] in
          Mutex_o.unlock ctl_m ;
          if wake then wake_up ();
          h'.return ())
    end
  end
  module Condition = 
  struct
    module Factory(Mutex : Mutex.Mutex_t) = 
    struct
      type condition = 
        { condition_m : Mutex_o.t ;
          waiting     : (unit -> unit) Queue.t }

      module Control = Mutex.Control

      let create () = 
        { condition_m = Mutex_o.create ();
          waiting = Queue.create () }

      (* Mutex.unlock m needs to happen _after_
       * the task has been registered. *)
      let wait c m = 
        (fun h ->
          let proc = 
            fun () -> Mutex.lock m h
          in
          Mutex_o.lock c.condition_m ;
          Queue.push proc c.waiting;
          Mutex_o.unlock c.condition_m ;
          (* Mutex.unlock does not raise exceptions (for now..) *)
          let h' = { return = (fun () -> ());
                     raise  = (fun _ -> assert false) }
          in
          Mutex.unlock m h') 

      let wake_up h = 
        let handler _ = h (); [] in
        Task.add Control.scheduler
         { Task.
            priority = Control.priority;
            events   = [`Delay 0.];
            handler  = handler }

      let signal c = 
        (fun h ->
          Mutex_o.lock c.condition_m;
          let h' = Queue.pop c.waiting in
          Mutex_o.unlock c.condition_m;
          wake_up h';
          h.return ())

      let broadcast c =
        (fun h ->
          let q = Queue.create () in
          Mutex_o.lock c.condition_m;
          Queue.transfer c.waiting q;
          Mutex_o.unlock c.condition_m;
          Queue.iter wake_up q;
          h.return ())
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

