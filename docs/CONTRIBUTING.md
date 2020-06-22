# How to contribute

We're really glad you're reading this, because we need volunteer developers to help this project come to fruition.

## Did you find a bug?

- **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/emqx/kuiper/issues).
- If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/emqx/kuiper/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## Submitting changes

The `develop` branch is active development branch, so it's recommended to set `develop` as base branch, and also create PR against `develop` branch.

When you create a pull request, we will love you forever if you include examples. We can always use more test coverage. Always write a clear log message for your commits. One-line messages are fine for small changes, but bigger changes should look like this:

```
$ git commit -m "A brief summary of the commit
> 
> A paragraph describing what changed and its impact."
```

### Testing

The Kuiper project leverages Github actions to run unit test & FVT (functional verification test), so please take a look at the PR status result, and make sure that all of testcases run successfully.

- Write Golang unit testcases to test your code if necessary.
- A set of [FVT testcases](../fvt_scripts/README.md) will be triggered with any PR submission, so please make sure that these testcases can be run successfully.

## Code conventions

- Use `go fmt` to format your code before commit code change. Kuiper Github Action CI pipeline reports error if it's not format by `go fmt`.
- Configuration key in config files uses camel case format.