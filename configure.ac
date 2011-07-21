
# check for one particular file of the sources 
AC_INIT(ocaml-duppy, 0.4.1, savonet-users@lists.sourceforge.net)

VERSION=$PACKAGE_VERSION
AC_SUBST(VERSION)
AC_MSG_RESULT([configuring $PACKAGE_STRING])

AC_CHECK_OCAML_COMPILERS()

AC_PROG_CC()
AC_CHECK_TOOL([AR],[ar],no)
AC_SUBST(AR)

AC_CHECK_OCAML_BINDING([pcre],[],[],[1])
INC="$INC `$OCAMLFIND query pcre`"

if test "$CAMLP4O" != "no" ; then
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
