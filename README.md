# Implementing a New Plugin for Polypheny

This repo adds support for using DuckDB as a data source in Polypheny and illustrates how to create a new plugin.

Most of the code of a Polypheny plugin goes inside the plugin folder, which in our example is `plugin/duckdb-adapter`. Similar to the file structure of other plugins, there are two gradle related files under this path along with the source code.

One of the file is `build.gradle`, which declares plugin-specific dependencies, defines the location of source files, and guides gradle to build this plugin. For example, in our duckdb-adapter, we have the following line in the dependencies section which states that this plugin requires the JDBC driver of DuckDB.

```
implementation group: 'org.duckdb', name: 'duckdb_jdbc', version: '0.10.1'
```

The other one gradle-related file is `gradle.properties`, which defines various variables of a plugin, including version, id, class, provider information, categories etc.

In our duckdb-adapter example, some variables defined in `gradle.properties`.

```
pluginVersion = 0.0.1

pluginId = duckdb-adapter
pluginClass = org.polypheny.db.duckdb.sources.DuckdbPlugin
pluginProvider = The Polypheny Project
pluginDependencies = jdbc-adapter-framework, sql-language
...
pluginCategories = source
...
```

Here `plugInCategories` defines the type of a plugin. If the plugin is a data store, then the value should be set to `store`. If the plugin adds a new query interface to Polypheny, then the value should be set to `interface`.

Polypheny provides many features to help the creation of new plugins. For example, the abstract class `AbstractJdbcSource` simplifies the process of adding a new relational data source.

When the implementation is finished, one needs to adapt the `settings.gradle` file, which is not located in the plugin folder, to include the new plugin. In our case, we added the following line:

```
include 'plugins:duckdb-adapter'
```