# MooFile for Java

MooFile is a lightweight embedded document store with MongoDB-style filters,
vector search, and BM25 text search. The Java binding uses the JDK Foreign
Function & Memory API—no JNI and no third-party Java dependencies.

- **JDK 22+ is required.** A JRE alone is insufficient because you need
  `javac` to compile against the binding.
- The binding needs a matching native `libmoofile` file at runtime.
- Java artifacts are published on the project's **GitHub Releases**, not Maven
  Central. This deliberately avoids the Sonatype namespace, GPG-signing, and
  staging-repository overhead while the Java binding is young.

## Download a release

Download both from the same [GitHub Release](https://github.com/patw/moofile/releases):

1. `moofile-java-<version>.jar`.
2. The native archive matching the **deployment** platform:

   | Deployment target | Native archive | Library in archive |
   |---|---|---|
   | Linux x64 | `moofile-linux-x86_64.tar.gz` | `lib/libmoofile.so` |
   | macOS Apple Silicon | `moofile-macos-aarch64.tar.gz` | `lib/libmoofile.dylib` |
   | Windows x64 | `moofile-windows-x86_64.zip` | `lib/moofile.dll` |

Verify the downloads against the release's `SHA256SUMS`, then unpack the native
archive. The library must remain a real file on disk at runtime.

## Use from `javac`

Create `App.java`:

```java
import com.moofile.Collection;
import com.moofile.Document;

import static com.moofile.Filters.*;

public class App {
    public static void main(String[] args) {
        try (Collection db = Collection.open("people.bson")) {
            db.insert(Document.of("name", "Alice", "age", 30, "status", "active"));

            for (Document person : db.find(and(gte("age", 18), eq("status", "active")))) {
                System.out.println(person);
            }
        }
    }
}
```

Compile it against the downloaded binding JAR:

```bash
javac -cp moofile-java-1.1.0.jar App.java
```

Run it with both required JVM options:

```bash
# macOS Apple Silicon
java \
  --enable-native-access=ALL-UNNAMED \
  -Dmoofile.library.path=moofile-macos-aarch64/lib/libmoofile.dylib \
  -cp moofile-java-1.1.0.jar:. \
  App

# Linux x64: replace the property value with
# moofile-linux-x86_64/lib/libmoofile.so
```

`--enable-native-access=ALL-UNNAMED` is required for the JDK FFM API.
`moofile.library.path` must name the actual matching `.dylib`, `.so`, or `.dll`.
You can set `MOOFILE_LIB` instead, but the JVM property is recommended because
it makes the deployment dependency explicit.

## Maven and Gradle projects

MooFile is not in Maven Central. Commit or otherwise provision the release JAR
with your application, then reference it as a local file.

### Gradle

Place the binding JAR in `libs/`:

```kotlin
// build.gradle.kts
dependencies {
    implementation(files("libs/moofile-java-1.1.0.jar"))
}
```

### Maven

Install the release JAR into the local Maven repository as part of developer or
CI setup:

```bash
mvn install:install-file \
  -Dfile=libs/moofile-java-1.1.0.jar \
  -DgroupId=com.moofile \
  -DartifactId=moofile \
  -Dversion=1.1.0 \
  -Dpackaging=jar
```

Then use a normal dependency:

```xml
<dependency>
  <groupId>com.moofile</groupId>
  <artifactId>moofile</artifactId>
  <version>1.1.0</version>
</dependency>
```

For reproducible team and CI builds, prefer publishing the downloaded JAR to
your organization's artifact repository (GitHub Packages, Artifactory, or
Nexus) over Maven's `system` scope.

## Fat / uber JAR applications

Maven Shade and Gradle Shadow can include `moofile-java-<version>.jar` in an
application fat JAR normally. The native library is different: **do not leave
it only inside the fat JAR**. Panama's `SymbolLookup.libraryLookup` requires a
filesystem path; a `jar:` resource URL will not work.

Package the correct native library next to your application instead:

```text
my-app/
├── app-all.jar
└── native/
    └── libmoofile.dylib       # macOS example
```

Launch it with a relative path:

```bash
java \
  --enable-native-access=ALL-UNNAMED \
  -Dmoofile.library.path=native/libmoofile.dylib \
  -jar app-all.jar
```

For a multi-platform distribution, publish one application bundle per target,
or have your installer choose and unpack the matching native library. Do not
ship a single universal bundle and rely on the JVM to load a library compiled
for another operating system or architecture.

## Build from source

For contributing or local binding development, the repository has no Maven or
Gradle build requirement:

```bash
# Builds production/test classes and runs tests. Builds libmoofile if absent.
bash bindings/java/build.sh test

# Runs the examples.
bash bindings/java/build.sh example

# Creates bindings/java/build/moofile-java-<version>.jar.
# This action needs only a JDK; it does not build the native library.
bash bindings/java/build.sh jar
```

The generated JAR contains Java classes only. Pair it with a native library
from the same source revision or release when deploying.
