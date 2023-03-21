Primus also works well with ByteDance [Monolith](https://github.com/bytedance/monolith), please
refer to [Primus Example](https://github.com/bytedance/monolith/tree/master/markdown/primus_demo)
from Monolith to see how Primus on Kubernetes is integrated with Monolith.

Notes

- Setting up [Primus Baseline VM](../../docs/primus-quickstart.md) on bare-metal Ubuntu 22.04 is
  suggested, since some hypervisors such as VirtualBox hinder CPU instruction sets required by
  Monolith prebuilt docker image.
- 20+ GB memory is required for the machine to host Monolith example.
- Increasing `"registerRetryTimes"` in Primus Configuration to 900 is encouraged as Monolith
  prebuilt docker image takes longer to launch.  
