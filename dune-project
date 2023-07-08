(lang dune 2.7)
(version 0.9.3)
(name duppy)
(source (github savonet/ocaml-duppy))
(license LGPL-2.1-only)
(authors "Romain Beauxis <toots@rastageeks.org>")
(maintainers "The Savonet Team <savonet-users@lists.sourceforge.net>")

(generate_opam_files true)

(package
 (name duppy)
 (synopsis "Library providing monadic threads")
 (depends
  (ocaml (>= 4.07.0))
  dune
  pcre)
)
