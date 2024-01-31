## Swagger Doc

Install the `swag` tool using the following command:

```shell
go install github.com/swaggo/swag/cmd/swag@latest
```

To generate Swagger documentation from code comments, run the following command:

```shell
swag init -g rest.go
```

For detailed information on how to write comments in the declarative format, refer to the [Declarative Comments Format documentation](https://github.com/swaggo/swag?tab=readme-ov-file#declarative-comments-format).