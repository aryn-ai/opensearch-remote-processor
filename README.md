# Remote Search Processor Plugin
This plugin adds a remote search processor that calls out to the sycamore search processing service.

### Editing the CI workflow
You may want to edit the CI of your new repo.
  
In your new GitHub repo, head over to `.github/workflows/CI.yml`. This file describes the workflow for testing new push or pull-request actions on the repo.
Currently, it is configured to build the plugin and run all the tests in it.

You may need to alter the dependencies required by your new plugin.
Also, the **OpenSearch version** in the `Build OpenSearch` and in the `Build and Run Tests` steps should match your plugins version in the `build.gradle` file.

To view more complex CI examples you may want to checkout the workflows in official OpenSearch plugins, such as [anomaly-detection](https://github.com/opensearch-project/anomaly-detection/blob/main/.github/workflows/test_build_multi_platform.yml).

## License
This code is licensed under the Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Copyright
Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
