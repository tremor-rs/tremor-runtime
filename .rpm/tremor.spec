# RPM spec file meant for packaging via cargo-rpm
#
# initially generated with `cargo rpm init` and then (heavily) modified for
# our use.

%define __spec_install_post %{nil}
%define __os_install_post %{_dbpath}/brp-compress
%define debug_package %{nil}

# if the _unitdir macro is not defined, set it to the standard systemd unit path
%{!?_unitdir: %define _unitdir /usr/lib/systemd/system}

###############################################################################

# the @@ strings here will be replaced with meaningful values when the build
# is done via cargo-rpm
Name: tremor
Summary: Tremor is an early stage event processing system for unstructured data with rich support for structural pattern matching, filtering and transformation.
Version: @@VERSION@@
Release: @@RELEASE@@%{?dist}
License: ASL 2.0
Group: System Environment/Daemons
Source0: %{name}-%{version}.tar.gz
URL: https://www.tremor.rs

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

###############################################################################

%if 0%{?centos}
# ensures systemd_* macros are available for use in rpm scripts (as in %post)
BuildRequires: systemd
%endif

# TODO make sure our package works on glibc 2.17 (for centos 7 support) for x86_64.
# the binary is built on image tremorproject/tremor-builder:x86_64-unknown-linux-gnu,
# (debian buster) which has glibc 2.28, so will probably need to try from older images
# with older glibc (and still ensure that tremor compiles there).
Requires: glibc >= 2.18
# TODO link to these statically?
# via snmalloc
Requires: libstdc++
Requires: libatomic
# via surf
Requires: libcurl
# via surf and rdkafka. provides libz
Requires: zlib
# via xz2 (in tremor-script). provides liblzma
Requires: xz

# for user/group creation
Requires(post): shadow-utils

Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd

###############################################################################

%description
%{summary}

%prep
%setup -q

%build
# we rely on binaries pre-built alrady in the Source0 tar file
# so this section is empty for us.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
cp -a * %{buildroot}
#tree %{_topdir} # only for debugging

%clean
rm -rf %{buildroot}

###############################################################################

%post
# create user/group for our use. based on:
# https://docs.fedoraproject.org/en-US/packaging-guidelines/UsersAndGroups/#_dynamic_allocation
getent group tremor >/dev/null || groupadd -r tremor
getent passwd tremor >/dev/null || \
  useradd -r -g tremor -d / -s /sbin/nologin \
  -c "Tremor" tremor

# create the log dir
mkdir -p %{_localstatedir}/log/%{name}
chown -R tremor:tremor %{_localstatedir}/log/%{name}

# systemd setup
%if 0%{?centos}
%systemd_post tremor.service
%else
# contents of systemd_post macro from centos7.7
if [ $1 -eq 1 ] ; then
  # Initial installation
  systemctl preset tremor.service >/dev/null 2>&1 || :
fi
%endif

###############################################################################

%preun
%if 0%{?centos}
%systemd_preun tremor.service
%else
# contents of systemd_preun macro from centos7.7
if [ $1 -eq 0 ] ; then
  # Package removal, not upgrade
  systemctl --no-reload disable tremor.service > /dev/null 2>&1 || :
  systemctl stop %{?*} > /dev/null 2>&1 || :
fi
%endif

%postun
%if 0%{?centos}
%systemd_postun_with_restart tremor.service
%else
# contents of systemd_postun macro from centos7.7
systemctl daemon-reload >/dev/null 2>&1 || :
if [ $1 -ge 1 ] ; then
  # Package upgrade, not uninstall
  systemctl try-restart tremor.service >/dev/null 2>&1 || :
fi
%endif

###############################################################################

%files
%defattr(-,root,root,-)

# the dir macros used here should line up with path names that are part of
# package.metadata.rpm.files configuration in the project's cargo manifest

%{_bindir}/*

%doc %{_datadir}/doc/%{name}/README.md
%license %{_datadir}/licenses/%{name}/LICENSE

%config(noreplace) %{_sysconfdir}/%{name}/logger.yaml
%config %{_sysconfdir}/%{name}/config/*

%dir %{_usr}/lib/tremor-script/
%{_usr}/lib/tremor-script/*

%{_unitdir}/tremor.service
