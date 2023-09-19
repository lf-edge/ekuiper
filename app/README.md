If you need to compile from source, please follow these instructions.
1. Install the Fyne dependencies according to the documentation provided by Fyne. https://developer.fyne.io/started/
2. Install the Android NDK, version r24 has been tested without problems, the other versions have not been tested.
3. Copy the `etc` folder from the parent directory to the current directory, `cp -r etc app`.
4. Go to the app directory, and start packaging.
   ``` bash
   ver=$(git describe --tags --always)
   fyne package -os android/arm64 -name ekuiper -metadata version=$ver -release -appID github.com.lfedge.ekuiper -icon icon.png
   ```
5. If all goes well you'll get `ekuiper.apk`.