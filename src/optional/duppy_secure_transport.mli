type secure_transport_socket = {
  ctx: SecureTransport.t;
  sock: Unix.file_descr
}
module Io : Duppy.Io_t with type socket = secure_transport_socket
module Monad_io : Duppy.Monad.Monad_io_t with type socket = secure_transport_socket
