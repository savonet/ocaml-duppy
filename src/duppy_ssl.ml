module Ssl_transport : Duppy.Transport_t with type t = Ssl.socket =
struct
  type t = Ssl.socket
  type bigarray = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
  let sock = Ssl.file_descr_of_socket
  let read = Ssl.read
  let write = Ssl.write
  let ba_write _ _ _ _ =
    failwith "Not implemented!"
end

module Io = Duppy.MakeIo(Ssl_transport)
module Monad_io = Duppy.Monad.MakeIo(Io)
