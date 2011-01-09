(* Ocaml-duppy syntax extension
 * Module Pa_duppy, based on an original work
 * from Jérémie Dimino.
 * Copyright (C) 2009 Jérémie Dimino
 * Copyright (C) 2010 Romain Beauxis
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, with linking exceptions;
 * either version 2.1 of the License, or (at your option) any later
 * version. See COPYING file for details.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 * 02111-1307, USA.
 *)

open Camlp4
open Camlp4.PreCast
open Syntax

let gen_binding l =
  let rec aux n = function
    | [] ->
        assert false
    | [(_loc, p, e)] ->
          <:binding< $lid:"__pa_duppy_" ^ string_of_int n$ = $e$ >>
    | (_loc, p, e) :: l ->
          <:binding< $lid:"__pa_duppy_" ^ string_of_int n$ = $e$ and $aux (n + 1) l$ >>
  in
  aux 0 l

let gen_bind l e =
  let rec aux n = function
    | [] ->
        e
    | (_loc, p, e) :: l ->
        <:expr< Duppy.Monad.bind $lid:"__pa_duppy_" ^ string_of_int n$ (fun $p$ -> $aux (n + 1) l$) >>
  in
  aux 0 l

let rec gen_do _loc e = 
  let rec aux = function
    | e :: [] -> e
    | e :: l  ->
        <:expr< Duppy.Monad.bind begin $e$ end 
                (fun () -> $aux l$) >>
    | [] -> <:expr< () >>
  in
  aux (Ast.list_of_expr e [])

let rec gen_write ~p h _loc e =
  let do_write e = 
    <:expr< Duppy.Monad.Io.write
                 ~priority:$p$
                 $h$ ($e$) >>
  in
  let rec aux = function
    | e :: [] -> do_write e
    | e :: l  ->
     <:expr< 
       Duppy.Monad.bind
                 ($do_write e$)
                 (fun () -> $aux l$) >>
    | [] -> <:expr< Duppy.Monad.return () >>
  in
  aux (Ast.list_of_expr e [])

exception Patt_assoc of Camlp4.PreCast.Syntax.Ast.expr

let patt_assoc s l =
  let f (p,x) = 
    match p with
      | <:patt< $lid:f$ >> when f = s -> 
          raise (Patt_assoc x)
      | _ -> ()
  in
  try
    List.iter f l ;
    raise Not_found
  with
    | Patt_assoc x -> x

EXTEND Gram
  GLOBAL: expr;

    letb_binding:
      [ [ b1 = SELF; "and"; b2 = SELF -> b1 @ b2
        | p = patt; "="; e = expr -> [(_loc, p, e)]
        ] ];

    duppy_match:
      [ [ p = patt; "="; v = expr LEVEL "top"; ";"; l = duppy_match -> (p,v) :: l
        | p = patt; "="; v = expr LEVEL "top" -> [(p,v)]
        ] ];

    expr: LEVEL "top"
      [ [ "duppy_try"; e = expr LEVEL ";"; "with"; c = match_case ->
            <:expr< Duppy.Monad.catch $e$ (function $c$) >>

        | "duppy_run"; e = expr LEVEL ";"; "with"; "{"; l = duppy_match; "}" ->
            let return,raise = 
              try
                patt_assoc "return" l,
                patt_assoc "raise"  l
              with
                | Not_found ->
                    invalid_arg ("Invalid arguments for duppy_run")
            in
            <:expr< Duppy.Monad.run ~return:$return$ ~raise:$raise$ $e$ >>

        | "duppy"; l = letb_binding; "in"; e = expr LEVEL ";" ->
            <:expr< let $gen_binding l$ in $gen_bind l e$ >>

        | "duppy_do"; e = do_sequence -> 
             <:expr< $gen_do _loc e$ >>

        | "duppy_iter" ->
             <:expr< Duppy.Monad.iter >>
 
        | "duppy_wait" ->
             <:expr< Duppy.Monad.Condition.wait >>

        | "duppy_broadcast" ->
            <:expr< Duppy.Monad.Condition.broadcast >>

        | "duppy_signal" ->
            <:expr< Duppy.Monad.Condition.signal >>

        | "duppy_condition" ->
           <:expr< Duppy.Monad.Condition.create >>

        | "duppy_lock" ->
             <:expr< Duppy.Monad.Mutex.lock >>

        | "duppy_try_lock" ->
             <:expr< Duppy.Monad.Mutex.lock >>

        | "duppy_unlock" ->
            <:expr< Duppy.Monad.Mutex.unlock >>

        | "duppy_delay" ->
            <:expr< Duppy.Monad.Io.delay >>

        | "duppy_mutex" ->
           <:expr< Duppy.Monad.Mutex.create >>

        | "duppy_fold_left" ->
             <:expr< Duppy.Monad.fold_left >>

        | "duppy_return"; e = expr LEVEL "top" ->
             <:expr< Duppy.Monad.return $e$ >>

        | "duppy_raise"; e = expr LEVEL "top" ->
             <:expr< Duppy.Monad.raise $e$ >>

        | "duppy_exec"; e = expr; "with"; "{"; l = duppy_match; "}" ->
             let p,h = 
               try
                 patt_assoc "priority" l,
                 patt_assoc "handler" l 
               with
                 | Not_found ->
                    invalid_arg ("Invalid arguments for duppy_exec")
             in
             <:expr< Duppy.Monad.Io.exec ~priority:$p$ $h$ $e$ >>

        | "duppy_write"; e = sequence; "with"; "{"; l = duppy_match; "}" ->
             let p,h =
               try
                 patt_assoc "priority" l,
                 patt_assoc "handler" l
               with
                 | Not_found ->
                    invalid_arg ("Invalid arguments for duppy_write")
             in
             gen_write ~p h _loc e

        | "duppy_write_bigarray"; e = expr; "with"; "{"; l = duppy_match; "}" ->
             let p,h =
               try
                 patt_assoc "priority" l,
                 patt_assoc "handler" l
               with
                 | Not_found ->
                    invalid_arg ("Invalid arguments for duppy_write_big_array")
             in
             <:expr< Duppy.Monad.Io.write_bigarray ~priority:$p$ $h$ $e$ >>

        | "duppy_read"; e = expr; "with"; "{"; l = duppy_match; "}" ->
             let p,h =
               try
                 patt_assoc "priority" l,
                 patt_assoc "handler" l
               with
                 | Not_found ->
                    invalid_arg ("Invalid arguments for duppy_read")
             in
             <:expr< Duppy.Monad.Io.read ~priority:$p$ ~marker:$e$ $h$ >>

        | "duppy_read_all"; e = expr; "with"; "{"; l = duppy_match; "}" ->
             let p,s =
               try
                 patt_assoc "priority" l,
                 patt_assoc "scheduler" l
               with
                 | Not_found ->
                    invalid_arg ("Invalid arguments for duppy_read_all")
             in
             <:expr< Duppy.Monad.Io.read_all ~priority:$p$ $s$ $e$ >>

        ] ];

END

