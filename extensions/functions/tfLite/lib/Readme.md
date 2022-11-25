# Tensorflow Lite C API library

This is the prebuilt tensorflow lite c library for debian 10. It can be used directly in eKuiper docker image of tags x.x.x or x.x.x-slim.

To use in other environment, you need to build the library from source.

## Build from source

Here are the steps to build from source in debian. 

1. Install [Python](https://www.tensorflow.org/install/pip#1.-install-the-python-development-environment-on-your-system)

2. Install required python lib: `pip3 install -r requirements.txt`. The requirements are from `tensorflow/tensorflow/tools/pip_package/setup.py` of the corresponding tensorflow version.

3. Install [Bazel](https://docs.bazel.build/versions/4.0.0/install-ubuntu.html)

4. Clone [tensorflow](https://github.com/tensorflow/tensorflow)，switch to `git checkout v2.2.0-rc3 -b mybranch`

5. Build the so files, the outputs are in ./bazel-bin

   ```bash
   $ cd $tensorflowSrc
   $ bazel build --config monolithic -c opt //tensorflow/lite:libtensorflowlite.so
   $ bazel build --config monolithic -c opt //tensorflow/lite/c:libtensorflowlite_c.so
   ```
