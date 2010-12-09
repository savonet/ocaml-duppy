
# check for one particular file of the sources 
AC_INIT(ocaml-duppy, 0.4.0, savonet-users@lists.sourceforge.net)

VERSION=$PACKAGE_VERSION
AC_SUBST(VERSION)
AC_MSG_RESULT([configuring $PACKAGE_STRING])

OCAMLFIND_LDCONF=""
AC_ARG_ENABLE([ldconf], AC_HELP_STRING([--disable-ldconf],[don't modify the dynamic loader configuration file (default is enable)]),[ac_enable_ldconf=$enableval],[ac_enable_ldconf=$enableval],[ac_enable_ldconf=yes])
if test "$ac_enable_ldconf" = no ; then
	AC_MSG_RESULT([disabling modification of ld.conf])
	OCAMLFIND_LDCONF=dummy
fi

# Check for Ocaml compilers

# we first look for ocamlc in the path; if not present, we fail
AC_CHECK_PROG(OCAMLC,ocamlc,`which ocamlc`,no)
if test "$OCAMLC" = no ; then
	AC_MSG_ERROR(Cannot find ocamlc.)
fi

# we look for the directory of ocamlc in $OCAMLC
OCAMLBIN=`dirname $OCAMLC`

# we extract Ocaml version number and library path
OCAMLVERSION=`$OCAMLC -v | sed -n -e 's|.*version* *\(.*\)$|\1|p' `
echo "ocaml version is $OCAMLVERSION"
OCAMLLIB=`$OCAMLC -v | tail -n 1 | cut -f 4 -d " "`
echo "ocaml library path is $OCAMLLIB"

# then we look for ocamlopt; if not present, we issue a warning
# if the version is not the same, we also discard it
# we set OCAMLBEST to "opt" or "byte", whether ocamlopt is available or not
AC_PATH_PROG(OCAMLOPT,ocamlopt,no)
OCAMLBEST=byte
if test "$OCAMLOPT" = no ; then
	AC_MSG_WARN(Cannot find ocamlopt; bytecode compilation only.)
else
	AC_MSG_CHECKING(ocamlopt version)
	TMPVERSION=`$OCAMLOPT -v | sed -n -e 's|.*version* *\(.*\)$|\1|p' `
	if test "$TMPVERSION" != "$OCAMLVERSION" ; then
	    AC_MSG_RESULT(differs from ocamlc; ocamlopt discarded.)
	    OCAMLOPT=no
	else
	    AC_MSG_RESULT(ok)
	    OCAMLBEST=opt
	fi
fi

# checking for ocamlc.opt
AC_PATH_PROG(OCAMLCDOTOPT,ocamlc.opt,no)
if test "$OCAMLCDOTOPT" != no ; then
	AC_MSG_CHECKING(ocamlc.opt version)
	TMPVERSION=`$OCAMLCDOTOPT -v | sed -n -e 's|.*version* *\(.*\)$|\1|p' `
	if test "$TMPVERSION" != "$OCAMLVERSION" ; then
	    AC_MSG_RESULT(differs from ocamlc; ocamlc.opt discarded.)
	else
	    AC_MSG_RESULT(ok)
	    OCAMLC=$OCAMLCDOTOPT
	fi
fi

# checking for ocamlopt.opt
if test "$OCAMLOPT" != no ; then
    AC_PATH_PROG(OCAMLOPTDOTOPT,ocamlopt.opt,no)
    if test "$OCAMLOPTDOTOPT" != no ; then
	AC_MSG_CHECKING(ocamlc.opt version)
	TMPVER=`$OCAMLOPTDOTOPT -v | sed -n -e 's|.*version* *\(.*\)$|\1|p' `
	if test "$TMPVER" != "$OCAMLVERSION" ; then
	    AC_MSG_RESULT(differs from ocamlc; ocamlopt.opt discarded.)
	else
	    AC_MSG_RESULT(ok)
	    OCAMLOPT=$OCAMLOPTDOTOPT
	fi
    fi
fi

AC_PATH_PROG(OCAMLDEP,ocamldep,no)
if test "$OCAMLDEP" = no ; then
	AC_MSG_ERROR(Cannot find ocamldep.)
fi

AC_PATH_PROG(OCAMLDOC,ocamldoc,no)

AC_ARG_ENABLE([debugging],
   AC_HELP_STRING(
      [--enable-debugging],
      [compile with debugging information (backtrace printing in particular)]))

if test "x$enable_debugging" = "xyes" ; then
  OCAMLFLAGS="$OCAMLFLAGS -g"
fi

AC_PATH_PROG(OCAMLFIND,ocamlfind,no)
if test "$OCAMLFIND" = no ; then
	AC_MSG_ERROR(Cannot find ocamlfind.)
fi

CAMLLIBPATH=`$OCAMLC -where`

AC_PROG_CC()

AC_MSG_CHECKING(for ocaml-pcre)
   if ! $OCAMLFIND query pcre > /dev/null 2>&1 ; then
       AC_MSG_ERROR(Not found.)
   else
   INC="$INC `$OCAMLFIND query pcre`"
   AC_MSG_RESULT(ok)
fi

AC_MSG_CHECKING(for camlp4)
   if ! $OCAMLFIND query camlp4 > /dev/null 2>&1 ; then
       AC_MSG_RESULT(Not found.)
   else
   if ! `which camlp4o > /dev/null 2>&1`; then
      AC_MSG_RESULT(Not found.)
   else
     PA_DUPPY="pa_duppy.cmo"
     PA_DOC="pa_duppy.mli"
     PA_DUPPY_META="package \"syntax\" (
  description = \"Syntactic sugars for Duppy\"
  requires = \"camlp4\"
  version=\"${VERSION}\"
  archive(syntax,preprocessor) = \"pa_duppy.cmo\"
  archive(syntax,toploop) = \"pa_duppy.cmo\"
)"
     AC_MSG_RESULT(ok)
  fi
fi

AC_SUBST(PA_DUPPY)
AC_SUBST(PA_DUPPY_META)
AC_SUBST(PA_DOC)

if test "$OCAMLOPT" = no ; then
    BEST=byte
else
    BEST="byte opt"
fi

# substitutions to perform
requires="unix threads pcre bigarray"
AC_SUBST(VERSION)
AC_SUBST(OCAMLC)
AC_SUBST(OCAMLOPT)
AC_SUBST(OCAMLFLAGS)
AC_SUBST(OCAMLDEP)
AC_SUBST(OCAMLBEST)
AC_SUBST(OCAMLVERSION)
AC_SUBST(OCAMLLIB)
AC_SUBST(OCAMLBIN)
AC_SUBST(OCAMLDOC)
AC_SUBST(OCAMLFIND)
AC_SUBST(OCAMLFIND_LDCONF)
AC_SUBST(OCAMLCP)
AC_SUBST(CAMLLIBPATH)
AC_SUBST(BEST)
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
