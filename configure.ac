AC_INIT([ocaml-duppy],[0.7.3],[savonet-users@lists.sourceforge.net])

VERSION=$PACKAGE_VERSION
AC_SUBST(VERSION)
AC_MSG_RESULT([configuring $PACKAGE_STRING])

AC_BASE_CHECKS()

AC_CHECK_OCAML_BINDING([pcre],[],[],[1])
INC="$INC `$OCAMLFIND query pcre`"

AC_CHECK_OCAML_BINDING([bytes],[],[],[1])
INC="$INC `$OCAMLFIND query bytes`"

AC_CHECK_OCAML_BINDING([ssl])

if test "$w_SSL" == "yes"; then
  INC="$INC `$OCAMLFIND query ssl`"
  DUPPY_SSL="duppy_ssl"
  DUPPY_SSL_META="package \"ssl\" (
  description = \"OCaml advanced scheduler (SSL module)\"
  requires = \"duppy ssl\"
  version=\"${VERSION}\"
  archive(byte) = \"duppy_ssl.cma\"
  archive(native) = \"duppy_ssl.cmxa\"
)"
fi
AC_SUBST(DUPPY_SSL)
AC_SUBST(DUPPY_SSL_META)

AC_CHECK_OCAML_BINDING([osx-secure-transport])

if test "$w_OSX_SECURE_TRANSPORT" == "yes"; then
  INC="$INC `$OCAMLFIND query osx-secure-transport`"
  DUPPY_SECURE_TRANSPORT="duppy_secure_transport"
  DUPPY_SECURE_TRANSPORT_META="package \"secure_transport\" (
  description = \"OCaml advanced scheduler (SecureTransport module)\"
  requires = \"duppy secure_transport\"
  version=\"${VERSION}\"
  archive(byte) = \"duppy_secure_transport.cma\"
  archive(native) = \"duppy_secure_transport.cmxa\"
)"
fi
AC_SUBST(DUPPY_SECURE_TRANSPORT)
AC_SUBST(DUPPY_SECURE_TRANSPORT_META)

AC_CHECK_OCAML_BINDING([camlp4])

if test "$w_CAMLP4" == "yes" ; then
  PA_DUPPY="pa_duppy.cmo"
  PA_DOC="pa_duppy.mli"
  PA_DUPPY_META="package \"syntax\" (
  description = \"Syntactic sugars for Duppy\"
  requires = \"camlp4\"
  version=\"${VERSION}\"
  archive(syntax,preprocessor) = \"pa_duppy.cmo\"
  archive(syntax,toploop) = \"pa_duppy.cmo\"
)"
fi

AC_SUBST(PA_DUPPY)
AC_SUBST(PA_DUPPY_META)
AC_SUBST(PA_DOC)

# substitutions to perform
requires="unix threads pcre bigarray"
AC_SUBST(VERSION)
ALL_INC="`ocamlfind query -r -separator " " $requires` $_INC"
INC="$INC $_INC"
AC_SUBST(INC)
AC_SUBST(ALL_INC)
all_requires="`ocamlfind query -r -separator " " -format "%p" $requires` $_requires"
requires="$requires $_requires"
AC_SUBST(requires)
AC_SUBST(all_requires)

# Finally create the Makefile and samples
AC_CONFIG_FILES([Makefile],[chmod a-w Makefile])
AC_CONFIG_FILES([src/META])
AC_CONFIG_FILES([src/Makefile])
AC_CONFIG_FILES([examples/Makefile.telnet])
AC_CONFIG_FILES([examples/Makefile.http])
AC_OUTPUT
