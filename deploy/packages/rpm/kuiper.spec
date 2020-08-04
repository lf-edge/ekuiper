%define debug_package %{nil}
%define _user %{_name}
%define _group %{_name}
%define _conf_dir %{_sysconfdir}/%{_name}
%define _log_dir %{_var}/log/%{_name}
%define _lib_home /usr/lib/%{_name}
%define _var_home %{_sharedstatedir}/%{_name}
%define _build_id_links none

Name: %{_package_name}
Version: %{_version}
Release: %{_release}%{?dist}
Summary: kuiper
Group: System Environment/Daemons
License: Apache License Version 2.0
URL: https://www.emqx.io
BuildRoot: %{_tmppath}/%{_name}-%{_version}-root
Provides: %{_name}
AutoReq: 0

%description
A lightweight IoT edge analytics software

%prep

%build
cd %{_code_source}
GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=%{_version}-%{_release} -X main.LoadFileType=absolute" -o %{_code_source}/cli %{_code_source}/xstream/cli/main.go
GO111MODULE=on CGO_ENABLED=1 go build -ldflags="-s -w -X main.Version=%{_version}-%{_release} -X main.LoadFileType=absolute" -o %{_code_source}/server %{_code_source}/xstream/server/main.go
cd -

%install
mkdir -p %{buildroot}%{_lib_home}/bin
mkdir -p %{buildroot}%{_log_dir}
mkdir -p %{buildroot}%{_unitdir}
mkdir -p %{buildroot}%{_conf_dir}
mkdir -p %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_var_home}
mkdir -p %{buildroot}%{_var_home}/data
mkdir -p %{buildroot}%{_var_home}/plugins
mkdir -p %{buildroot}%{_initddir}


cp %{_code_source}/deploy/packages/service_helper.sh %{buildroot}%{_lib_home}/bin/
cp %{_code_source}/cli %{buildroot}%{_lib_home}/bin/
cp %{_code_source}/server %{buildroot}%{_lib_home}/bin/
cp -R %{_code_source}/etc/* %{buildroot}%{_conf_dir}/
cp -R %{_code_source}/plugins/* %{buildroot}%{_var_home}/plugins/
install -m644 %{_service_src} %{buildroot}%{_service_dst}

%pre
if [ $1 = 1 ]; then
  # Initial installation
  /usr/bin/getent group %{_group} >/dev/null || /usr/sbin/groupadd -r %{_group}
  if ! /usr/bin/getent passwd %{_user} >/dev/null ; then
      /usr/sbin/useradd -r -g %{_group} -m -d %{_sharedstatedir}/%{_name} -c "%{_name}" %{_user}
  fi
fi

%post
if [ $1 = 1 ]; then
    ln -s %{_lib_home}/bin/server %{_bindir}/kuiperd
    ln -s %{_lib_home}/bin/cli %{_bindir}/kuiper
fi
%{_post_addition}
if [ -e %{_initddir}/%{_name} ] ; then
    /sbin/chkconfig --add %{_name}
else
    systemctl enable %{_name}.service
fi

%preun
%{_preun_addition}
# Only on uninstall, not upgrades
if [ $1 = 0 ]; then
    if [ -e %{_initddir}/%{_name} ] ; then
        /sbin/service %{_name} stop > /dev/null 2>&1
        /sbin/chkconfig --del %{_name}
    else
        systemctl disable %{_name}.service
    fi
    rm -f %{_bindir}/kuiperd
    rm -f %{_bindir}/kuiper
fi
exit 0

%files
%defattr(-,root,root)
%{_service_dst}
%{_lib_home}
%attr(0700,%{_user},%{_group}) %dir %{_var_home}
%attr(0755,%{_user},%{_group}) %config(noreplace) %{_var_home}/*
%attr(0755,%{_user},%{_group}) %dir %{_log_dir}
%attr(0755,%{_user},%{_group}) %config(noreplace) %{_conf_dir}/*

%clean
rm -rf %{buildroot}

%changelog

