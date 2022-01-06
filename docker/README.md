# Create a custom Docker for running builds

You can modify the build script and test it locally by:
```
docker build -t test_build .
```

Whenever it works fine, you can push it to any repository
```
 docker login -u "$USER" -p "$PASSWORD" $REGISTRY
 docker build --pull -t "$REGISTRY_IMAGE" .
 docker push "$REGISTRY_IMAGE"
```