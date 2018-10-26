- Python 2.7 
  - Python 3.x正在开发中

- 模块管理
  - 模块化开发，服从反射框架规范

- 代码管理
  - http://git.intra.mosaicbo.com/datastrategy/mosaicclient

- Commit 信息按照功能填写
  - 功能强化  前缀 ： - [ENH]
  - Bug修复   前缀 ： - [BUG]
  - 文档完善  前缀 ： - [DOC]
  - 代码重构  前缀 ： - [CLN]
  - 其他操作  前缀 ： - [REVERT] / - [MERG] / - [UPD] / - [TEST]


- 版本管理
  - 平均两到三周一个小版本
  - 重大功能更新可优先发版
  - 重大代码调整可升级第二位版本
  - 0.5.4.1
    - 0 是平台目前内测，系统版本号
    - 5 是mosaic平台
    - 4 是mosaicclient 主版本号
    - 1 是mosaicclient 子版本号

# Contributing to Weiclient
Welcome to [report Issues](http://git.intra.mosaicbo.com/datastrategy/mosaicclient/issues) or [pull requests](http://git.intra.mosaicbo.com/datastrategy/mosaicclient/pulls). It's recommended to read the following Contributing Guide first before contributing.


## Issues
We use Gitlab Issues to track public bugs and feature requests.

### Search Known Issues First
Please search the existing issues to see if any similar issue or feature request has already been filed. You should make sure your issue isn't redundant.

### Reporting New Issues
If you open an issue, the more information the better. Such as detailed description, screenshot or video of your problem, logcat or code blocks for your crash.

## Pull Requests
We strongly welcome your pull request to make Weiclient better.

Ensure you have signed the [Contributor License Agreement (CLA).](master/CLA.md)


### Branch Management
There are three main branches here:

1. `master` branch.

	(1). It is the latest (pre-)release branch. We use `master` for tags, with version number `0.4.2.1`, `0.5.5.1`, `0.5.5.2`...

	(2). **Don't submit any PR on `master` branch.**

2. `specific version` branchs.

	(1).There is a `specific version` for each Weiclient version, such as `mosaicclient-0.5.2.x`, `mosaicclient-0.5.5.x`. It is our stable developing	 branch. After full testing, `specific version` branch will be merged to `master` branch for the next release.

	(2). **You are recommended to submit bugfix or feature PR on `specific version` branch.**


Normal bugfix or feature request should be submitted to `specific version` branch. After full testing, we will merge them to `master` branch for the next release.


### Make Pull Requests
The code team will monitor all pull request, we run some code check and test on it. After all tests passed, we will accecpt this PR. But it won't merge to `master` branch at once, which have some delay.

Before submitting a pull request, please make sure the followings are done:

1. Fork the repo and create your branch from `master` or `specific version`.
2. Update code or documentation if you have changed APIs.
3. Add the copyright notice to the top of any new files you've added.
4. Check your code lints and checkstyles.
5. Test and test again your code.
6. Now, you can submit your pull request on  `specific version` branch.

## Code Style Guide
Use Code Style .

## License
By contributing to Weiclient, you agree that your contributions will be licensed

