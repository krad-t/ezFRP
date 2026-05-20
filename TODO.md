# ezFRP — TODO

## v0.1.0 ✅ 已完成
- [x] TCP Echo 示例（server + client）
- [x] 基础 TCP 隧道（单连接双向转发）
- [x] 控制通道 + 数据通道分离

## v0.2.0 — 进行中 (feature/multi-client)
- [x] 支持多个外部用户同时连接
- [x] 每个外部用户独立的数据通道
- [x] 外部用户断开时，对应的数据通道线程优雅退出

## v0.3.0
- [ ] UDP 隧道支持（MC 联机需要）
- [ ] Server 同时监听 TCP + UDP

## v0.4.0
- [ ] 配置文件支持（YAML/JSON），不再硬编码端口
- [ ] Client 端可配置本地服务地址

## v0.5.0
- [ ] Docker 部署（Dockerfile + docker-compose.yml）
- [ ] Server 一键部署到 VPS

## 远期（可选）
- [ ] 控制通道心跳/重连
- [ ] C++ 数据转发热路径
- [ ] 简单 Web Dashboard
