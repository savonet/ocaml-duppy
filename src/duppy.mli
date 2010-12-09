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
    * Finally, {!Duppy.Monad} and {!Duppy.Monad.Io} provide a monadic interface to
    * program server code that with an implicit return/reply execution flow.
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

(** Core task registration.
  *
  * A task will be a set of events to watch, and a corresponding function to
  * execute when one of the events is trigered.
  * 
  * The executed function may then return a list of new tasks to schedule. *)
module Task : 
sig

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

(** Asynchronous task module
  *
  * This module implements an asychronous API to {!Duppy.scheduler}
  * It allows to create a task that will run and then go to sleep. *)
module Async :
sig

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
    * you can use [wake_up] to resume the task and execute [f] again. 
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

(** Easy parsing of [Unix.file_descr].
  *
  * With {!Duppy.Io.read}, you can pass a file descriptor to the scheduler,
  * along with a marker, and have it run the associated function when the
  * marker is found.
  *
  * With {!Duppy.Io.write}, the schdeduler will try to write recursively to the file descriptor
  * the given string. *)
module Io :
sig

  (** Type for markers.
    *
    * [Split s] recognizes all regexp allowed by the 
    * [Pcre] module. *)
  type marker = Length of int | Split of string

  (** Type of [Bigarray] used here. *)
  type bigarray = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  (** Different types of failure.
    * 
    * [Io_error] is raised when reading or writing
    * returned 0. This usually means that the socket
    * was closed. *)
  type failure = Io_error | Unix of Unix.error*string*string | Unknown of exn

  (** Wrapper to perform a read on a socket and trigger a function when
    * a marker has been detected, or enough data has been read.
    * It reads recursively on a socket, splitting into strings seperated
    * by the marker (if any) and calls the given function on the list of strings.
    * 
    * Can be used recursively or not, depending on the way you process strings. 
    * Because of Unix's semantic, it is not possible to stop reading
    * at first marker, so there can be a remaining string. If not used
    * recursively, the second optional argument may contain a remaining
    * string. You should then initiate the next read with this value.
    *
    * The [on_error] function is used when reading failed on the socket.
    * Depending on your usage, it can be a hard failure, or simply a lost client.
    * @param recursive recursively read and process, default: [true]
    * @param init initial string for reading, default: [""]
    * @param on_error function used when read failed, default: [fun _ -> ()] *)
  val read :
        ?recursive:bool -> ?init:string -> ?on_error:(failure -> unit) ->
        priority:'a -> 'a scheduler -> Unix.file_descr -> 
        marker -> (string*(string option) -> unit) -> unit

  (** Similar to [read] but less complex.
    * [write ?exec ?on_error ?string ?bigarray ~priority scheduler socket] 
    * write data from [string], or from [bigarray] is no string is given, 
    * to [socket], and executes [exec] or [on_error] if errors occured.
    * @param exec function to execute after writing, default: [fun () -> ()] 
    * @param on_error function to execute when an error occured, default: [fun _ -> ()] 
    * @param string write data from this string 
    * @param bigarray write data from this bigarray, if no [string] is given *)
  val write :
        ?exec:(unit -> unit) -> ?on_error:(failure -> unit) -> 
        ?bigarray:bigarray -> ?string:string -> priority:'a -> 
        'a scheduler -> Unix.file_descr -> unit
end

(** Monadic interface to {!Duppy.Io}. 
  * 
  * This module can be used to write code
  * that runs in various Duppy's tasks and
  * raise values in a completely transparent way.
  * 
  * You can see examples of its use
  * in the [examples/] directory of the
  * source code and in the files 
  * [src/tools/{harbor.camlp4,server.camlp4}]
  * in liquidsoap's code. 
  * 
  * When a server communicates
  * with a client, it performs several
  * computations and, eventually, terminates.
  * A computation can either return a new 
  * value or terminate. For instance:
  * 
  * - Client connects.
  * - Server tries to authenticate the client.
  * - If authentication is ok, proceed with the next step.
  * - Otherwise terminate.
  *
  * The purpose of the monad is to embed
  * computations which can either return 
  * a new value or raise a value that is used
  * to terminate. *)
module Monad : 
sig

  (** Type representing a computation
    * which returns a value of type ['a] 
    * or raises a value of type ['b] *) 
  type ('a,'b) t

  (** A reply function takes a value of type ['a]
    * raised during the execution. The reply
    * function is executed when the computation
    * terminates. *)
  type 'a reply = 'a -> unit

  (** [return x] create a computation that 
    * returns value [x]. *)
  val return : 'a -> ('a,'b) t

  (** [raise x] create a computation that raises
    * value [x]. *)
  val raise  : 'b -> ('a,'b) t

  (** Compose two computations. 
    * [bind f g] is equivalent to:
    * [let x = f in g x] where [x] 
   * has f's return type. *)
  val bind : ('a,'b) t -> ('a -> ('c,'b) t) -> ('c,'b) t

  (** [>>=] is an alternative notation
    * for [bind] *)
  val (>>=) : ('a,'b) t -> ('a -> ('c,'b) t) -> ('c,'b) t

  (** [run f rep] executes [f] and process 
    * the last value [x], returned or raised, 
    * with [rep]. *)
  val run  : ('a,'a) t -> 'a reply -> unit

  (** [catch f g] redirects values [x] raised during
    * [f]'s execution to [g]. The name suggests the
    * usual [try .. with ..] exception catching. *)
  val catch : ('a,'b) t -> ('b -> ('a,'c) t) -> ('a,'c) t

  (** [fold_left f a [b1; b2; ..]] returns computation 
    * [ (f a b1) >>= (fun a -> f a b2) >>= ...] *) 
  val fold_left : ('a -> 'b -> ('a,'c) t) -> 'a -> 'b list -> ('a,'c) t

  (** [iter f [x1; x2; ..]] returns computation
    * [f x1 >>= (fun () -> f x2) >>= ...] *)
  val iter : ('a -> (unit,'b) t) -> 'a list -> (unit,'b) t

  (** This module implements monadic computations
    * using [Duppy.Io]. It can be used to create
    * computations that read or write from a socket,
    * and also to redirect a computation in a different
    * queue with a new priority. *)
  module Io : 
  sig
    (** A handler for this module
      * is a record that contains the
      * required elements. In particular,
      * [on_error] is a function that transforms
      * an error raised by [Duppy.Io] to a reply
      * used to terminate the computation.
      * [init] is used internaly. It should 
      * be initialized with [""] and should
      * not be used in user's code. It contains the
      * remaining data that was received when 
      * using [read] *) 
    type ('a,'b) handler =
      { scheduler              : 'a scheduler ;
        socket                 : Unix.file_descr ;
        mutable init           : string ;
        on_error               : Io.failure -> 'b }

    (** [exec ?delay ~priority h f x] redirects computation
      * [f] into a new queue with priority [priority] and
      * delay [delay] ([0.] by default).
      * It can be used to redirect a computation that
      * has to run under a different priority. For instance,
      * a computation that reads from a socket is generally
      * not blocking because the function is executed
      * only when some data is available for reading. 
      * However, if the data that is read needs to be processed
      * by a computation that can be blocking, then one may 
      * use [exec] to redirect this computation into an 
      * appropriate queue. *)
    val exec : ?delay:float -> priority:'a -> ('a,'b) handler ->
               ('c -> ('d,'b) t) -> 'c -> ('d,'b) t

    (** [read ~priority ~marker h] creates a 
      * computation that reads from [h.socket]
      * and returns the first string split 
      * according to [marker]. This function
      * can be used to create a computation that
      * reads data from a socket. *)
    val read : priority:'a -> marker:Io.marker -> 
               ('a,'b) handler -> (string,'b) t

    (** [write ~priority h s] creates a computation
      * that writes string [s] to [h.socket]. This
      * function can be used to create a computation
      * that sends data to a socket. *)
    val write : priority:'a -> ('a,'b) handler -> 
                string -> (unit,'b) t

    (** [write_bigarray ~priority h ba] creates a computation
      * that writes data from [ba] to [h.socket]. This function
      * can to create a computation that writes data to a socket. *)
    val write_bigarray : priority:'a -> ('a,'b) handler ->
                         Io.bigarray -> (unit,'b) t
  end
end

  (** {2 Some culture..}
    * {e Duppy is a Caribbean patois word of West African origin meaning ghost or spirit.
    * Much of Caribbean folklore revolves around duppies. 
    * Duppies are generally regarded as malevolent spirits. 
    * They are said to come out and haunt people at night mostly, 
    * and people from the islands claim to have seen them. 
    * The 'Rolling Calf', 'Three footed horse' or 'Old Higue' are examples of the more malicious spirits. }
    * {R {{:http://en.wikipedia.org/wiki/Duppy} http://en.wikipedia.org/wiki/Duppy}}*)
