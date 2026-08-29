# Windows下编译

## 前置依赖

Windows构建需要CMake、OpenSSL和C++编译器。OpenSSL可以使用源码、安装包、Chocolatey或vcpkg安装。使用源码或安装包时，在CMake配置阶段通过`OPENSSL_ROOT_DIR`指定安装目录。

MSVC构建可以使用Visual Studio默认生成器，也可以在已经能找到`cl.exe`的开发者命令行中使用Ninja。仓库不依赖固定的Visual Studio、vcpkg或MSYS2安装路径。

## 官方构建目录

Windows分支使用以下目录布局：

```text
build.cmake             # workflow库
test\build.cmake
tutorial\build.cmake
```

这些目录是单配置缓存。切换Debug/Release或切换生成器前，删除对应的`build.cmake`目录。

## MSVC

有Visual Studio的用户可以使用默认生成器：

```powershell
cmake -S . -B build.cmake -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build build.cmake --config Debug
```

使用MSVC命令行工具和Ninja：

```powershell
cmake -S . -B build.cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build build.cmake --target workflow --parallel
```

## 测试

测试需要安装GTest，并通过`GTest_DIR`指定其CMake包：

```powershell
cmake -S test -B test\build.cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DGTest_DIR=[gtest install]\lib\cmake\GTest -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build test\build.cmake --parallel
ctest --test-dir test\build.cmake --output-on-failure
```

## Tutorial

```powershell
cmake -S tutorial -B tutorial\build.cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=.;[openssl directory] -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build tutorial\build.cmake --parallel
```

Tutorial可执行文件输出到`tutorial`源码目录，保持官方Windows分支的布局。

## MinGW

官方GNUmakefile支持MinGW。在MSYS2 shell中从仓库根目录执行：

```bash
make MINGW=y DEBUG=y
make -C test MINGW=y DEBUG=y check
make -C tutorial MINGW=y DEBUG=y
```
