[Workflow英文介绍](https://github.com/sogou/workflow/blob/master/README.md)

# Windows下编译

Workflow在Windows上使用CMake构建，并依赖OpenSSL。请先安装CMake、OpenSSL和对应的C++编译器。

## 安装OpenSSL

可以使用源码、安装包、Chocolatey或vcpkg安装OpenSSL。使用源码或安装包时，在配置CMake时通过`OPENSSL_ROOT_DIR`指定安装目录。

## 编译workflow库

仓库的构建入口是CMake，并沿用官方Windows分支的目录布局：

```text
build.cmake             # workflow库
test\build.cmake
tutorial\build.cmake
```

有Visual Studio的用户可以使用默认生成器：

```powershell
cmake -S . -B build.cmake -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build build.cmake --config Debug
```

没有使用Visual Studio生成器时，可以在已经能找到`cl.exe`的开发者命令行中直接使用MSVC和Ninja：

```powershell
cmake -S . -B build.cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build build.cmake --target workflow --parallel
```

测试需要安装GTest，并通过`GTest_DIR`指定其CMake包：

```powershell
cmake -S test -B test\build.cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DGTest_DIR=[gtest install]\lib\cmake\GTest -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build test\build.cmake --parallel
ctest --test-dir test\build.cmake --output-on-failure
```

tutorial使用相同的官方目录：

```powershell
cmake -S tutorial -B tutorial\build.cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=.;[openssl directory] -DOPENSSL_ROOT_DIR=[openssl directory]
cmake --build tutorial\build.cmake --parallel
```

Ninja缓存是单配置的。切换Debug/Release或MSVC/MinGW前，删除对应的`build.cmake`目录。仓库不记录依赖本机Visual Studio、vcpkg或MSYS2安装路径的构建脚本。

## MinGW

官方GNUmakefile仍然支持MinGW。在MSYS2 shell中，从仓库根目录执行：

```bash
make MINGW=y DEBUG=y
make -C test MINGW=y DEBUG=y check
make -C tutorial MINGW=y DEBUG=y
```

## VCPKG安装workflow

如果只使用Workflow而不修改源码，可以通过vcpkg安装：

```powershell
vcpkg install workflow
```

更多信息请参考[vcpkg官方文档](https://learn.microsoft.com/vcpkg/)。
