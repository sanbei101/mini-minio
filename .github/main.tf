terraform {
  required_providers {
    alicloud = {
      source  = "aliyun/alicloud"
      version = "~> 1.290"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.9.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.9.1"
    }
  }
}

provider "alicloud" {
  region = "cn-hangzhou"
}

variable "resource_group_id" {
  type        = string
  default     = "rg-aek62w3cegjhi5a"
  description = "CI/CD 专用的阿里云资源组 ID"
}

data "alicloud_zones" "default" {
  available_resource_creation = "Instance"
  available_instance_type     = "ecs.e-c1m1.large"
  available_disk_category     = "cloud_essd"
  spot_strategy               = "SpotAsPriceGo"
}

data "alicloud_images" "ubuntu" {
  name_regex  = "^ubuntu_24_04"
  owners      = "system"
  most_recent = true
}

resource "alicloud_vpc" "vpc" {
  vpc_name          = "spot-test-vpc"
  cidr_block        = "172.16.0.0/16"
  resource_group_id = var.resource_group_id
}

resource "alicloud_vswitch" "vswitch" {
  vpc_id     = alicloud_vpc.vpc.id
  cidr_block = "172.16.1.0/24"
  zone_id    = data.alicloud_zones.default.zones[0].id
}

resource "alicloud_security_group" "sg" {
  security_group_name = "spot-test-sg"
  vpc_id              = alicloud_vpc.vpc.id
  resource_group_id   = var.resource_group_id
}

resource "alicloud_security_group_rule" "allow_ssh" {
  type              = "ingress"
  ip_protocol       = "tcp"
  port_range        = "22/22"
  security_group_id = alicloud_security_group.sg.id
  cidr_ip           = "0.0.0.0/0"
}

resource "alicloud_security_group_rule" "allow_internal_all" {
  type              = "ingress"
  ip_protocol       = "all"
  port_range        = "-1/-1"
  security_group_id = alicloud_security_group.sg.id
  cidr_ip           = alicloud_vswitch.vswitch.cidr_block
}

resource "tls_private_key" "ssh_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "local_file" "private_key" {
  content         = tls_private_key.ssh_key.private_key_pem
  filename        = "${path.module}/id_rsa_spot"
  file_permission = "0600"
}

resource "random_id" "tag" {
  byte_length = 4
}

resource "alicloud_key_pair" "key" {
  key_pair_name     = "spot-test-key-${random_id.tag.hex}"
  public_key        = tls_private_key.ssh_key.public_key_openssh
  resource_group_id = var.resource_group_id
}

resource "alicloud_instance" "spot_nodes" {
  count                      = 2
  availability_zone          = data.alicloud_zones.default.zones[0].id
  security_groups            = [alicloud_security_group.sg.id]
  instance_type              = "ecs.e-c1m1.large"
  system_disk_category       = "cloud_essd"
  system_disk_size           = 20
  image_id                   = data.alicloud_images.ubuntu.images[0].id
  instance_name              = "spot-cluster-node-${count.index}"
  vswitch_id                 = alicloud_vswitch.vswitch.id
  internet_max_bandwidth_out = 5

  instance_charge_type = "PostPaid"
  spot_strategy        = "SpotAsPriceGo"
  auto_release_time    = timeadd(timestamp(), "1h")
  resource_group_id    = var.resource_group_id
  key_name             = alicloud_key_pair.key.key_pair_name

  lifecycle {
    ignore_changes = [auto_release_time]
  }
}

resource "alicloud_ecs_disk" "data" {
  # ponytail: system disk stores d1; add a second data disk per node for isolated storage benchmarks.
  count                = 2
  zone_id              = data.alicloud_zones.default.zones[0].id
  disk_name            = "spot-test-data-${count.index}"
  category             = "cloud_essd"
  performance_level    = "PL1"
  size                 = 20
  payment_type         = "PayAsYouGo"
  delete_with_instance = true
  resource_group_id    = var.resource_group_id
}

resource "alicloud_ecs_disk_attachment" "data" {
  count = length(alicloud_ecs_disk.data)

  disk_id     = alicloud_ecs_disk.data[count.index].id
  instance_id = alicloud_instance.spot_nodes[count.index].id
}

resource "alicloud_instance" "spot_bench" {
  availability_zone          = data.alicloud_zones.default.zones[0].id
  security_groups            = [alicloud_security_group.sg.id]
  instance_type              = "ecs.e-c1m1.large"
  system_disk_category       = "cloud_essd"
  system_disk_size           = 20
  image_id                   = data.alicloud_images.ubuntu.images[0].id
  instance_name              = "spot-bench-client"
  vswitch_id                 = alicloud_vswitch.vswitch.id
  internet_max_bandwidth_out = 5

  instance_charge_type = "PostPaid"
  spot_strategy        = "SpotAsPriceGo"
  auto_release_time    = timeadd(timestamp(), "1h")
  resource_group_id    = var.resource_group_id
  key_name             = alicloud_key_pair.key.key_pair_name

  lifecycle {
    ignore_changes = [auto_release_time]
  }
}

output "node_ips" {
  description = "节点的公网与私网 IP 映射"
  value = {
    for idx, inst in alicloud_instance.spot_nodes :
    "node_${idx}" => {
      public_ip  = inst.public_ip
      private_ip = inst.primary_ip_address
    }
  }
}

output "bench_ip" {
  description = "独立发压机的公网与私网 IP"
  value = {
    public_ip  = alicloud_instance.spot_bench.public_ip
    private_ip = alicloud_instance.spot_bench.primary_ip_address
  }
}

output "data_disk_devices" {
  description = "各节点数据盘的 Linux 设备路径"
  value = {
    for index, attachment in alicloud_ecs_disk_attachment.data :
    "node_${index}_d2" => attachment.device
  }
}

output "ssh_commands" {
  description = "各节点 SSH 登录命令"
  value = concat(
    [
      for inst in alicloud_instance.spot_nodes :
      "ssh -i id_rsa_spot root@${inst.public_ip}"
    ],
    ["ssh -i id_rsa_spot root@${alicloud_instance.spot_bench.public_ip}"]
  )
}

output "cluster_startup_commands" {
  description = "两台节点对应的启动命令示例"
  value = {
    node_0 = format(
      "./mini-minio --addr :9000 --node-url http://%s:9000 --cluster-secret test-cluster-secret --data-blocks 2 --parity-blocks 2 --endpoint http://%s:9000/data/d1 --endpoint http://%s:9000/data/d2 --endpoint http://%s:9000/data/d1 --endpoint http://%s:9000/data/d2",
      alicloud_instance.spot_nodes[0].primary_ip_address,
      alicloud_instance.spot_nodes[0].primary_ip_address,
      alicloud_instance.spot_nodes[0].primary_ip_address,
      alicloud_instance.spot_nodes[1].primary_ip_address,
      alicloud_instance.spot_nodes[1].primary_ip_address
    )
    node_1 = format(
      "./mini-minio --addr :9000 --node-url http://%s:9000 --cluster-secret test-cluster-secret --data-blocks 2 --parity-blocks 2 --endpoint http://%s:9000/data/d1 --endpoint http://%s:9000/data/d2 --endpoint http://%s:9000/data/d1 --endpoint http://%s:9000/data/d2",
      alicloud_instance.spot_nodes[1].primary_ip_address,
      alicloud_instance.spot_nodes[0].primary_ip_address,
      alicloud_instance.spot_nodes[0].primary_ip_address,
      alicloud_instance.spot_nodes[1].primary_ip_address,
      alicloud_instance.spot_nodes[1].primary_ip_address
    )
  }
}
