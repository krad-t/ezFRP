# ezFRP — TODO

## v0.1.0 ✅ 已完成
- [x] TCP Echo 示例（server + client）
- [x] 基础 TCP 隧道（单连接双向转发）
- [x] 控制通道 + 数据通道分离

## v0.2.0 ✅ 已完成
- [x] 支持多个外部用户同时连接
- [x] 每个外部用户独立的数据通道
- [x] 外部用户断开时，对应的数据通道线程优雅退出
- [x] Server 端控制通道断线重连

## v0.3.0 ✅ 已完成
- [x] UDP 完整隧道（带 session 状态管理）
- [x] Server 同时监听 TCP + UDP
- [x] Server 维护外部用户地址映射表
- [x] 模拟 UDP 应用测试（fake_udp_app）

## v0.4.0
- [ ] 配置文件支持（YAML/JSON），不再硬编码端口
- [ ] Client 端可配置本地服务地址
- [ ] 重构指令系统，从str封装

## v0.5.0
- [ ] Selector 技术替换 threading（I/O 多路复用）

## v0.6.0
- [ ] 多 Client 支持（Server 同时服务多个 Client）
- [ ] 每 Client 独立公网端口 + 端口注册协议
- [ ] Client 注册时声明"我需要服务端口 XXXX"

## v0.7.0
- [ ] Docker 部署（Dockerfile + docker-compose.yml）
- [ ] Server 一键部署到 VPS

## v0.8.0
- [ ] Client 端可视化界面
- [ ] Server 端 Web Dashboard

## v0.9.0
- [ ] 尝试实现P2P功能

## 远期（可选）
- [ ] 控制通道心跳/重连
- [ ] C++ 数据转发热路径