## Package from source
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

## Display non-ascii characters
Fyne does not have built-in support for non-ascii characters, which means that if you need to switch the content in the UI to Chinese, the UI will be garbled without a font package.
If you need to support non-ascii characters, please follow these steps.
1. Download a font package into the app folder.
   For example, [Smiley Sans](https://github.com/atelier-anchor/smiley-sans/releases).
   Save `SmileySans-Oblique.ttf` at the app folder. The relative path is `app\SmileySans-Oblique.ttf`.
2. Change the filename of `theme.go.example` to `theme.go`. Modify the `go:embed` parameter of `fontFile` to have the corresponding name.
3. Add a custom theme for the application in main.go.
   ``` go
   application.Settings().SetTheme(&myTheme{})
   ```