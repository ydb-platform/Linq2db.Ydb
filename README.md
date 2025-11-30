
> ### About the `Community.Ydb.Linq2db` NuGet package
>
> There is **no** dedicated `ydb-platform/Linq2db.Ydb` NuGet package in this repository,
> because the current YDB provider implementation is a **snapshot of ongoing work with the upstream**
> [`linq2db`](https://github.com/linq2db/linq2db) project.
>
> At the moment, the provider is distributed via an external community package:
>
> - NuGet: `Community.Ydb.Linq2db`
> - Source code and releases: https://github.com/poma12390/Linq2db.Ydb
>
> The goal is to evolve the YDB support to the point where it can be merged into the main
> [`linq2db`](https://github.com/linq2db/linq2db) repository.
>
> Because of that, **we do not publish a separate `ydb-platform` NuGet package**, in order to avoid
> future duplication and conflicting implementations: once the provider is accepted upstream, it will be
> available directly from the official `linq2db` packages.
