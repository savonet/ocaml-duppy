type secure_transport_socket = {
  ctx: SecureTransport.t;
  sock: Unix.file_descr
}
module Ssl_transport : Duppy.Transport_t with type t = secure_transport_socket =
struct
  type t = secure_transport_socket
  type bigarray = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
  let sock {sock} = sock
  let read {ctx} buf ofs len =
    SecureTransport.read ctx buf ofs len
  let write {ctx} buf ofs len =
    SecureTransport.write ctx buf ofs len
  let ba_write _ _ _ _ =
    failwith "Not implemented!"
end

module Io = Duppy.MakeIo(Ssl_transport)
module Monad_io = Duppy.Monad.MakeIo(Io)
