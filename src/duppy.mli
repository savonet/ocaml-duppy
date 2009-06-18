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

  (**  
    * {R {i {v 
    *        The bars could not hold me;
    *        Force could not control me now.
    *        They try to keep me down, yeah!
    *        But Jah put I around.
    *        (...)
    *        Let me tell you this -
    *        I'm a duppy conqueror !
    *        v}  }  } 
    * {R {b Lee "Scratch" Perry & Bob Marley - Duppy conqueror }}
    *
    * {2 Duppy task scheduler for OCaml.}
    *
    * {!Duppy} is a task scheduler for ocaml. It implements a wrapper 
    * around [Unix.select].
    * 
    * Using {!Duppy.Task}, the programmer can easily submit tasks that need to wait 
    * on a socket even, or for a given timeout (possibly zero).
    *
    * With {!Duppy.Async}, one can use a scheduler to submit asynchronous tasks.
    *
    * {!Duppy.Io} implements recursive easy reading and writing to a [Unix.file_descr]
    *
    * The scheduler can use several queues running concurently, each queue 
    * processing ready tasks. Of course, a queue should run in its own thread.*)

(** A scheduler is a device for processing tasks. Several queues might run in
  * different threads, processing one scheduler's tasks. 
  *
  * ['a] is the type of objects used for priorities. *)
type 'a scheduler

(** Initiate a new scheduler 
  * @param compare the comparison function used to sort tasks according to priorities. 
  * Works as in [List.sort] *)
val create : ?compare:('a -> 'a -> int) -> unit -> 'a scheduler 

(** [queue ~log ~priorities s name] 
 * starts a queue, on the scheduler [s] only processing priorities [p]
 * for which [priorities p] returns [true].
 *
 * Several queues can be run concurrently against [s]. 
 * @param log Logging function. Default: [Printf.printf "queue %s: %s\n" name]
 * @param priorities Predicate specifying which priority to process. Default: [fun _ -> _ -> true]*)
val queue :
    ?log:(string -> unit) -> ?priorities:('a -> bool) -> 
    'a scheduler -> string -> unit

(** Stop all queues running on that scheduler, causing them to return. *)
val stop : 'a scheduler -> unit

module Task : 
sig
  (** This modules implements the core task registration.
    *
    * A task will be a set of events to watch, and a corresponding function to
    * execute when one of the events is trigered.
    *
    * The executed function may then return a list of new tasks to schedule. *)


  (** A task is a list of events awaited,
    * and a function to process events that have occured.
    *
    * The ['a] parameter is the type of priorities, ['b] will be a subset of possible
    * events. *)
  type ('a,'b) task = {
    priority : 'a ;
    events   : 'b list ;
    handler  : 'b list -> ('a,'b) task list
  }

  (** Type for possible events. *)
  type event = [
    | `Delay of float
    | `Write of Unix.file_descr
    | `Read of Unix.file_descr
    | `Exception of Unix.file_descr
  ]
  
  (** Schedule a task. *)
  val add :
      'a scheduler -> ('a,[< event ]) task -> unit
end

module Async :
sig

  (** Asynchronous task module for {!Duppy}
    *
    * This module implements an asychronous API to {!Duppy.scheduler}
    * It allows to create a task that will run and then go to sleep. *)

  type t

  (** Exception raised when trying to wake_up a task 
    * that has been previously stopped *)
  exception Stopped

  (** [add ~priority s f] creates an asynchronous task in [s] with
    * priority [priority].
    *
    * The task executes the function [f].
    * If the task returns a positive float, the function will be executed
    * again after this delay. Otherwise it goes to sleep, and 
    * you can use wake_up] to resume the task and execute [f] again. 
    * Only a single call to [f] is done at each time. 
    * Multiple [wake_up] while previous task has not 
    * finished will result in sequentialized calls to [f]. *)
  val add : priority:'a -> 'a scheduler -> (unit -> float) -> t

  (** Wake up an asynchronous task. 
    * Raises [Stopped] if the task has been stopped. *)
  val wake_up : t -> unit

  (** Stop and remove the asynchronous task. Doesn't quit a running task. 
    * Raises [Stopped] if the task has been stopped. *)
  val stop : t -> unit
end

module Io :
sig

  (** This module implements an easy parsing of [Unix.file_descr].
    *
    * With {!Duppy.Io.read}, you can pass a file descriptor to the scheduler, 
    * along with a marker, and have it run the associated function when the 
    * marker is found.
    *
    * With {!Duppy.Io.write}, the schdeduler will try to write recursively to the file descriptor
    * the given string.*)

  (** Type for markers.
    *
    * [Split s] recognizes all regexp allowed by the 
    * [Pcre] module. *)
  type marker = Length of int | Split of string

  (** Different types of failure.
    * 
    * [Int d] is raised when reading or writing failed.
    * the returned value is respectively the ammount of 
    * data read or written *)
  type failure = Int of int | Unix of Unix.error*string*string | Unknown of exn

  (** Wrapper to perform a read on a socket and trigger a function when
    * a marker has been detected, or enough data has been read.
    * It reads recursively on a socket, splitting into strings seperated
    * by the marker (if any) and calls the given function on the list of strings.
    * 
    * Can be used recursively or not, depending on the way you process strings. 
    * Because of Unix's semantic, it is not possible to stop reading
    * at first marker, so there can be a remaining string. If not used
    * recursively, this string will be the last string passed in the list.
    * You should then initiate the next read with this value.
    *
    * The [on_error] function is used when reading failed on the socket.
    * Depending on your usage, it can be a hard failure, or simply a lost client.
    * @param recursive recursively read and process, default: [true]
    * @param init initial string for reading, default: [""]
    * @param on_error function used when read failed, default: [fun _ -> ()] *)
  val read :
        ?recursive:bool -> ?init:string -> ?on_error:(failure -> unit) ->
        priority:'a -> 'a scheduler -> Unix.file_descr -> 
        marker -> (string list -> unit) -> unit

  (** Similar to [read] but less complex.
    * Simply write a string to the socket, and then launch some callback.
    * @param exec function to execute after writing, default: [fun () -> ()] *)
  val write :
        ?exec:(unit -> unit) -> ?on_error:(failure -> unit) -> priority:'a -> 
        'a scheduler -> Unix.file_descr -> string -> unit
end


  (** {2 Some culture..}
    * {e Duppy is a Caribbean patois word of West African origin meaning ghost or spirit.
    * Much of Caribbean folklore revolves around duppies. 
    * Duppies are generally regarded as malevolent spirits. 
    * They are said to come out and haunt people at night mostly, 
    * and people from the islands claim to have seen them. 
    * The 'Rolling Calf', 'Three footed horse' or 'Old Higue' are examples of the more malicious spirits. }
    * {R {{:http://en.wikipedia.org/wiki/Duppy} http://en.wikipedia.org/wiki/Duppy}}*)
