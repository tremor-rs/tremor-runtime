Name:       tremor
Version:    1.0.0
Release:    1
Summary:    Tremor runtime
License:    FIXME
Source:     tremor-1.0.0.tar.gz



%description
Tremor runtime

Install eject if you'd like to eject removable media using
software control.

%prep
%setup -q

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/usr/bin
mkdir -p $RPM_BUILD_ROOT/usr/lib/tremor
mkdir -p $RPM_BUILD_ROOT/etc/tremor
mkdir -p $RPM_BUILD_ROOT/etc/tremor

install -s -m 755 tremor-runtime $RPM_BUILD_ROOT/usr/bin/tremor-runtime
cp tremor-runtime-svc.sh $RPM_BUILD_ROOT/usr/lib/tremor/tremor-runtime-svc.sh
cp tremor.conf.sample $RPM_BUILD_ROOT/etc/tremor/tremor.conf.sample

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)


/usr/bin/tremor-runtime
/usr/lib/tremor/tremor-runtime-svc.sh
/etc/tremor/tremor.conf.sample

%changelog
* Mon Aug 27 2018 hgies
- Initial release

[ Some changelog entries trimmed for brevity.  -Editor. ]

