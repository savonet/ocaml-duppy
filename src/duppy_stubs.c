/*
 * Copyright 2010 Savonet team
 *
 * This file is part of Ocaml-duppy.
 *
 * Ocaml-duppy is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * Ocaml-duppy is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Ocaml-duppy; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#include <caml/signals.h>
#include <caml/unixsupport.h>
#include <caml/memory.h>
#include <caml/bigarray.h>

#include <errno.h>

CAMLprim value ocaml_duppy_write_ba(value _fd, value ba, value _ofs, value _len)
{
  CAMLparam1(ba) ;
  int fd = Int_val(_fd);
  long ofs = Long_val(_ofs);
  long len = Long_val(_len);
  void *buf = Caml_ba_data_val(ba);
  int ret;

  int written = 0;
  while (len > 0) {
    caml_enter_blocking_section();
    ret = write(fd, buf+ofs, len);
    caml_leave_blocking_section();
    if (ret == -1) {
      if ((errno == EAGAIN || errno == EWOULDBLOCK) && written > 0) break;
      uerror("write", Nothing);
    }
    written += ret;
    ofs += ret;
    len -= ret;
  }

  CAMLreturn(Val_long(len));
}
