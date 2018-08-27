Name:       tremor
Version:    1.0.0
Release:    1
Summary:    Tremor runtime
License:    LICENSE
Source:     tremor-1.0.0.tar.gz
Requires(pre): shadow-utils


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
mkdir -p $RPM_BUILD_ROOT/etc/tremor
mkdir -p $RPM_BUILD_ROOT/usr/lib/systemd/system

install -s -m 755 tremor-runtime $RPM_BUILD_ROOT/usr/bin/tremor-runtime
cp tremor-runtime-svc.sh $RPM_BUILD_ROOT/usr/bin/tremor-runtime-svc.sh
cp tremor.conf.sample $RPM_BUILD_ROOT/etc/tremor/tremor.conf.sample
cp tremor.service $RPM_BUILD_ROOT/usr/lib/systemd/system/tremor.service

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%doc LICENSE

/usr/bin/tremor-runtime
/usr/bin/tremor-runtime-svc.sh
/usr/lib/systemd/system/tremor.service
/etc/tremor/tremor.conf.sample

%pre
getent group tremor >/dev/null || groupadd -r tremor
getent passwd tremor >/dev/null || \
    useradd -r -g tremor -s /sbin/nologin \
            -c "Tremor service user" tremor
exit 0

%post

if [ ! -f /etc/tremor/tremor.conf ]
then
    cp /etc/tremor/tremor.conf.sample /etc/tremor/tremor.conf
fi


%changelog
* Mon Aug 27 2018 hgies
- Initial release

[ Some changelog entries trimmed for brevity.  -Editor. ]

