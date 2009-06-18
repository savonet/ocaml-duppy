(*****************************************************************************

  Duppy, a task scheduler for OCaml.
  Copyright 2003-2008 Savonet team

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
  
  let add_t s item =
    Mutex.lock s.tasks_m ;
    s.tasks <- item :: s.tasks ;
    Mutex.unlock s.tasks_m ;
    wake_up s
  
  let add s t  = add_t s (t_of_task t)
end

open Task

let stop s = 
  s.stop <- true ;
  wake_up s

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
    let timeout = if e.t = infinity then -1. else max 0. (e.t -. (time ())) in
    log (Printf.sprintf "Enter select at %f, timeout %f (%d/%d/%d)."
           (time ()) timeout
           (List.length e.r) (List.length e.w) (List.length e.x)) ;
    let r,w,x =
      Thread.select e.r e.w e.x timeout
    in
      log (Printf.sprintf "Left select at %f (%d/%d/%d)." (time ())
             (List.length r) (List.length w) (List.length x)) ;
      r,w,x
  in
  (* Empty the wake_up pipe if needed. *)
  let () =
    if List.mem s.out_pipe r then
      (* Never mind if it only absorbs one wake_up. *)
      ignore (Unix.read s.out_pipe " " 0 1)
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
      List.iter (add_t s) (task ()) ;
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
  type t = (bool ref*fd option ref)

  exception Stopped

  let add ~priority (scheduler:'a scheduler) f = 
   (* A pipe to wake up the task *)
   let out_pipe,in_pipe = Unix.pipe () in
   let stop = ref false in
   let rec task l =
      if List.exists ((=) (`Read out_pipe)) l then
        (* Consume a character in the pipe *)
        ignore (Unix.read out_pipe " " 0 1) ;
      if !stop then begin
        begin 
          try
            Unix.close in_pipe 
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
   (stop,ref (Some in_pipe))

  let wake_up (_,t) =
    match !t with
      | Some t -> ignore (Unix.write t " " 0 1)
      | None -> raise Stopped

  let stop (b,t) = 
    if !t = None then raise Stopped;
    b := true ;
    wake_up (b,t) ;
    t := None
end

module Io = 
struct
  type marker = Length of int | Split of string
  type failure = Int of int | Unix of Unix.error*string*string | Unknown of exn

  let read ?(recursive=false) ?(init="") ?(on_error=fun _ -> ())
           ~priority (scheduler:'a scheduler) socket marker exec = 
    let rec f acc l =
      assert (List.exists ((=) (`Read socket)) l) ;
      let length =
        match marker with
          | Length n -> n - (String.length acc)
          | _ -> 100
      in
      let buf = String.make length ' ' in
      let input = Unix.read socket buf 0 length in
      if input<=0 then (on_error (Int input) ; [])
      else
        let acc = acc ^ (String.sub buf 0 input) in
        let l,acc = 
          match marker with
            | Split r ->
                  let rex = Pcre.regexp r in
                  let ret = Pcre.full_split ~rex acc in
                  (* A matched split list must contain at least 
                   * a text and a separator *)
                  if List.length ret < 2 then
                    [],acc
                  else
                    begin
                      (* Extract last matched field if 
                       * not followed by a delimiter *)
                      let ret,s = 
                        match List.rev ret with
                          | Pcre.Text s :: l -> List.rev l,s
                          | _ -> ret,""
                      in
                      let process l e = 
                        match e with
                          | Pcre.Text s -> s :: l
                          | _ -> l
                      in
                      let l = 
                        List.fold_left process [] ret 
                      in
                      if not recursive then
                        List.rev (s :: l),""
                      else
                        List.rev l,s
                    end
            | Length n when n <= String.length acc -> 
               let len = String.length acc in
               let rec f ret l =
                 if ret >= n then (l,ret)
                 else f (ret-n) ((String.sub acc (len - ret) n) :: l)
               in
               let l,ret = f len [] in
               let s = String.sub acc (len - ret) ret in
               if not recursive then
                   List.rev (s::l),""
               else
                 List.rev l,s
            | _ -> [],acc
        in
          if l <> [] then
            begin
              exec l ;
              if recursive then
                [{ priority = priority ; events = [`Read socket] ;
                   handler = f acc }]
              else
                []
            end
          else
            [{ priority = priority ; events = [`Read socket] ;
               handler = f acc }]
    in
    (* Catch all exceptions.. *)
    let f x y = 
      try
        f x y 
      with
        | Unix.Unix_error(x,y,z) -> on_error (Unix(x,y,z)); []
        | e -> on_error (Unknown e); []
    in
      let task = 
       {
         priority = priority ;
         events = [`Read socket] ;
         handler  = (f init)
       }
       in
      add scheduler task

  let write ?(exec=fun () -> ()) ?(on_error=fun _ -> ()) ~priority
            (scheduler:'a scheduler) socket s = 
    let rec f acc l = 
      assert (List.exists ((=) (`Write socket)) l) ;
      let length = String.length acc in
      let n = Unix.write socket s 0 length in
      if n<=0 then (on_error (Int n) ; [])
      else
      begin
        if n < length then
          begin
            let s = String.sub s n (n-length) in
              [{ priority = priority ; events = [`Write socket] ;
                 handler = f s }]
          end
        else
          (exec () ; [])
      end
    in  
    (* Catch all exceptions.. *)
    let f x y =
      try
        f x y
      with
        | Unix.Unix_error(x,y,z) -> on_error (Unix(x,y,z)); []
        | e -> on_error (Unknown e); []
    in
    if s <> "" then
        let task = 
          {
            priority = priority ;
            events = [`Write socket] ;
            handler  = (f s)
          }
        in
        add scheduler task
    else 
        exec ()
end
