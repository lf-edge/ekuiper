# Tensorflow Lite C API library

This is the prebuilt tensorflow lite c library for debian 10. It can be used directly in eKuiper docker image of tags
x.x.x or x.x.x-slim.

To use in other environment, you need to build the library from source.

## Current Lib Version

Built from

- Tensor Flow v2.18.1
- Ubuntu 18.04 (Compatible for lower version glibc)

## Build from source

Recommend to use cmake for newer version.

### Newer Version

https://android.googlesource.com/platform/external/tensorflow/+/6b511124eb0/tensorflow/lite/g3doc/guide/build_cmake.md

1. Install compilation tools
```shell
apt install cmake flatbuffers-compiler build-essential
```

2. Download tensor flow
```shell
cd /opt/tf
git clone https://github.com/tensorflow/tensorflow.git tensorflow_src
cd tensorflow_src
git checkout -b 218 v2.18.1
cd ..
```

3. Build TensorFlow Lite C library for AMD64
```shell
mkdir build_amd
cd build_amd
cmake -DTFLITE_ENABLE_XNNPACK=OFF ../tensorflow_src/tensorflow/lite/c
cmake --build . -j
```

4. Install cross compilation tool set

```shell
apt install gcc-aarch64-linux-gnu g++-aarch64-linux-gnu
```

Add below cmake file arm64.cmake

```cmake
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)

# Don't use automatic sysroot detection
set(CMAKE_C_COMPILER_WORKS TRUE)
set(CMAKE_CXX_COMPILER_WORKS TRUE)

# Set the target environment path
set(CMAKE_FIND_ROOT_PATH /usr/aarch64-linux-gnu)

# Adjust the default behavior of the FIND_XXX() commands
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
```

5. Build TensorFlow Lite C library for ARM64
```shell
mkdir build_arm
cd build_arm
cmake -DCMAKE_TOOLCHAIN_FILE=/opt/tf/arm64.cmake -DTFLITE_HOST_TOOLS_DIR=/usr/bin/flatc -DTFLITE_ENABLE_XNNPACK=OFF ../tensorflow_src/tensorflow/lite/c
cmake --build . -j
```

### Older Version

Here are the steps to build from source in debian.

1. Install [Python](https://www.tensorflow.org/install/pip#1.-install-the-python-development-environment-on-your-system)

2. Install required python lib: `pip3 install -r requirements.txt`. The requirements are
   from `tensorflow/tensorflow/tools/pip_package/setup.py` of the corresponding tensorflow version.

3. Install [Bazel](https://docs.bazel.build/versions/4.0.0/install-ubuntu.html)

4. Clone [tensorflow](https://github.com/tensorflow/tensorflow)，switch to `git checkout v2.2.0-rc3 -b mybranch`

5. Build the so files, the outputs are in ./bazel-bin

   ```bash
   $ cd $tensorflowSrc
   $ bazel build --config monolithic -c opt //tensorflow/lite:libtensorflowlite.so
   $ bazel build --config monolithic -c opt //tensorflow/lite/c:libtensorflowlite_c.so
   ```
