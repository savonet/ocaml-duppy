module Io : Duppy.Io_t with type socket = Ssl.socket
module Monad_io : Duppy.Monad.Monad_io_t with type socket = Ssl.socket
