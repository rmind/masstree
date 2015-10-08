Name:		libmasstree
Version:	0.1
Release:	1%{?dist}
Summary:	QSBR-based reclamation library
Group:		System Environment/Libraries
License:	BSD
URL:		https://github.com/rmind/masstree
Source0:	libmasstree.tar.gz

BuildRequires:	make
BuildRequires:	libtool

%description
Masstree is a lockless cache-aware trie of B+ trees.

%prep
%setup -q -n src

%build
make %{?_smp_mflags} LIBDIR=%{_libdir}

%install
make install \
    DESTDIR=%{buildroot} \
    LIBDIR=%{_libdir} \
    INCDIR=%{_includedir} \
    MANDIR=%{_mandir}

%files
%{_libdir}/*
%{_includedir}/*
#%{_mandir}/*

%changelog
