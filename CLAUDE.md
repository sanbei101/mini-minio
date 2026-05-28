# mini-minio 开发指南

## 项目简介
这是一个mini版的minio实现,目的是做一个精简版的minio,以便于学习和理解minio的核心功能和设计理念

## `抄袭`要求
- 我将`minio`项目下载到了`./copy`文件夹,需要`抄袭`原版的实现方式,但不能**复制**代码!
- 我只需要实现minio中的最最核心的功能,我把需要实现的接口功能`cmd/object-api-interface.go`都列了出来
- 在复刻`minio`的过程中,你需要判断哪些是不需要的,比如:
  - 为了兼容历史包袱的冗余判断代码
  - 审计日志相关的代码
  - 为了实现高级功能的代码
- 最后我需要使用`s3`兼容的api实现下列功能:
  - `GetObject`
  - `PresignGetObject`
  - `PutObject`
  - `PresignPutObject`
  - `MultipartUpload`
  - `DeleteObject`
  - `ListObjects`
  - `CreateBucket`
  - `DeleteBucket`
  - `ListBuckets`
其他的功能暂时不需要实现,先把核心功能实现

- 需要保留`minio`原版的文件路径结构,但文件内容需要重新编写,不能直接复制
- 需要使用`Go`语言编写,并且代码风格需要符合`Go`的最新语法规范
- 仍然使用`https://github.com/klauspost/reedsolomon`这个原版纠删码库,抄袭原版`minio`的`ObjectLayer`的时候,
  需要保留原版的高性能的代码,汲取原版的优秀设计理念,但是不需要那些为了安全和兼容性而写的冗余代码

## 规范
- 每次修改完成代码,需要运行`golangci-lint run --fix`检查代码质量并自动解决,需要保证没有任何lint错误
- `unused-parameter`的lint错误可以先不解决,但需要说明为什么没有使用,然后给我报告
- 运行完`lint`之后还需要运行`golangci-lint fmt`格式化代码,保证代码风格一致
- 最后运行`go test ./...`确保所有测试用例都通过