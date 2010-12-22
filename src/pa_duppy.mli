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

(** {2 Syntactic sugar for {!Duppy.Monad} }
  *
  * This module provides syntactic extensions to OCaml
  * using Camlp4 used to write code using {!Duppy.Monad}.
  *
  * It provides the following extensions:
  *
  * {2 Main Monad }
  * 
  * {ul
  * {- {[ duppy v = 
  *   foo
  *  in
  *  bar x ]}
  *
  * is equivalent to:
  * 
  * [Duppy.Monad.bind foo (fun x -> bar x)] 
  * 
  * }
  * {- {[ duppy_run
  *     foo
  *   with
  *     { return = f ;
  *       raise  = g } ]}
  *
  * is equivalent to:
  *
  * [Duppy.Monad.run ~return:f ~raise:g ()]
  * 
  * } 
  * {- {[ duppy_try
  *     foo
  *   with
  *     | a -> f
  *     | b -> g ]}
  *
  * is equivalent to:
  *
  * {[ Duppy.Monad.catch
  *     foo
  *     (function
  *         | a -> f
  *         | b -> g) ]} 
  * 
  * }
  * {- [duppy_fold_left] is equivalent to [Duppy.Monad.fold_left]
  *
  * }
  * {- [duppy_iter] is equivalent to [Duppy.Monad.iter]
  *
  * }
  * {- [duppy_return] is equivalent to [Duppy.Monad.return]
  *
  * }
  * {- [duppy_raise] is equivalent to [Duppy.Monad.raise]
  * 
  * }
  * {- {[ duppy_do
  *   foo ;
  *   bar ;
  *   ...
  * done ]}
  *
  * is equivalent to:
  *
  * {[ Duppy.Monad.bind
  *     (Duppy.Monad.bind 
  *       foo 
  *       (fun () -> bar))
  *     (fun () -> ...) ]}
  *
  * }}
  * 
  * {2 Mutex module }
  *
  * {ul
  * {- [duppy_mutex] is equivalent to [Duppy.Monad.Mutex.create]
  *
  * }
  * {- [duppy_lock] is equivalent to [Duppy.Monad.Mutex.lock]
  *
  * }
  * {- [duppy_try_lock] is equivalent to [Duppy.Monad.Mutex.try_lock]
  *
  * }
  * {- [duppy_unlock] is equivalent to [Duppy.Monad.Mutex.unlock]
  *
  * }}
  *
  * {2 Condition module }
  * {ul
  * {- [duppy_condition] is equivalent to [Duppy.Monad.Condition.create]
  *
  * }
  * {- [duppy_wait] is equivalent to [Duppy.Monad.Condition.wait]
  *
  * }
  * {- [duppy_signal] is equivalent to [Duppy.Monad.Condition.signal]
  *
  * }
  * {- [duppy_broadcast] is equivalent to [Duppy.Monad.Condition.broadcast]
  *
  * }}
  *
  * 
  * {2 I/O module }
  *
  * {ul
  * {- {[duppy_exec
  *  foo
  * with
  *   { priority = p ;
  *     handler  = h 
  *     delay    = d } ]}
  *
  * is equivalent to:
  *
  * {[ Duppy.Monad.Io.exec
  *  ~priority:p ~delay:d h foo ]} 
  * 
  * [delay] is an optional field.
  *
  * }
  * {- {[duppy_read
  *  marker
  * with
  *   { priority = p ;
  *     handler  = h } ]} 
  *
  * is equivalent to:
  *
  * {[ Duppy.Monad.Io.read
  *  ~priority:p h marker ]}
  *
  * }
  * {- {[duppy_write
  *  s
  * with
  *   { priority = p ;
  *     handler  = h } ]}
  *
  * is equivalent to:
  *
  * {[ Duppy.Monad.Io.write
  *  ~priority:p ~string:s h ]}
  *
  * }
  * {- {[duppy_write_bigarray
  *  ba
  * with
  *   { priority = p ;
  *     handler  = h } ]}
  *
  * is equivalent to:
  *
  * {[ Duppy.Monad.Io.write
  *  ~priority:p ~bigarray:ba h ]}
  *
  * }}
  *)
