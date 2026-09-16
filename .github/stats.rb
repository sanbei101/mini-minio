#!/usr/bin/env ruby
# 解析 action 运行中收集的 cpu,io,网络

action   = ARGV[0] # "start" 或 "dump"
tag      = ARGV[1] # "put" 或 "get"
nodes    = [ENV["NODE0_PUB"], ENV["NODE1_PUB"]].compact
ssh_opts = ENV["SSH_OPTS"] || "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i id_rsa_spot"

case action
when "start"
  cmd = "env LC_ALL=C nohup iostat -x 2 13 > /root/#{tag}-iostat.log 2>&1 & " \
        "env LC_ALL=C nohup mpstat -P ALL 2 13 > /root/#{tag}-mpstat.log 2>&1 & " \
        "env LC_ALL=C nohup sar -n DEV 2 13 > /root/#{tag}-net.log 2>&1 &"
  nodes.each do |ip|
    system("ssh #{ssh_opts} root@#{ip} #{cmd.inspect}")
  end

when "dump"
  puts "##### #{tag} resource usage #####"
  nodes.each do |ip|
    # 解析磁盘
    puts "--- #{ip} disks ---"
    iostat_raw = `ssh #{ssh_opts} root@#{ip} "cat /root/#{tag}-iostat.log" 2>/dev/null`
    headers = []
    disks = Hash.new { |h, k| h[k] = { r: 0.0, w: 0.0, u: 0.0, n: 0 } }
    report_num = 0

    iostat_raw.each_line do |line|
      cols = line.split
      next if cols.empty?
      if cols[0] == "Device"
        headers = cols
        report_num += 1
      elsif report_num > 1 && (cols[0] =~ /^[vs]d[a-z]$|^nvme\d+n\d+$/)
        r_idx = headers.index("rkB/s")
        w_idx = headers.index("wkB/s")
        u_idx = headers.index("%util")
        dev = cols[0]
        if r_idx && w_idx && u_idx
          disks[dev][:r] += cols[r_idx].to_f
          disks[dev][:w] += cols[w_idx].to_f
          disks[dev][:u] += cols[u_idx].to_f
          disks[dev][:n] += 1
        end
      end
    end

    disks.keys.sort.each do |dev|
      n = disks[dev][:n]
      next if n.zero?
      puts sprintf("%s avg: read=%.1fMB/s write=%.1fMB/s util=%.0f%%",
                  dev, disks[dev][:r] / n / 1024.0, disks[dev][:w] / n / 1024.0, disks[dev][:u] / n)
    end

    # 解析 CPU
    puts "--- #{ip} cpu ---"
    mpstat_raw = `ssh #{ssh_opts} root@#{ip} "cat /root/#{tag}-mpstat.log" 2>/dev/null`
    cpu_headers = []
    usr, sys, iow, soft, n_cpu = 0.0, 0.0, 0.0, 0.0, 0

    mpstat_raw.each_line do |line|
      cols = line.split
      next if cols.empty?
      if cols.include?("CPU")
        cpu_pos = cols.index("CPU")
        cpu_headers = cols[cpu_pos..]
      elsif cpu_headers.any? && cols.include?("all")
        all_pos = cols.index("all")
        u_idx  = cpu_headers.index("%usr")
        s_idx  = cpu_headers.index("%sys")
        io_idx = cpu_headers.index("%iowait")
        so_idx = cpu_headers.index("%soft")
        if u_idx && s_idx && io_idx && so_idx
          # 优先使用 mpstat 尾部的 Average 行
          if cols[0] =~ /^(Average:|平均时间:)/
            usr = cols[all_pos + u_idx].to_f
            sys = cols[all_pos + s_idx].to_f
            iow = cols[all_pos + io_idx].to_f
            soft = cols[all_pos + so_idx].to_f
            n_cpu = 1
            break
          else
            usr  += cols[all_pos + u_idx].to_f
            sys  += cols[all_pos + s_idx].to_f
            iow  += cols[all_pos + io_idx].to_f
            soft += cols[all_pos + so_idx].to_f
            n_cpu += 1
          end
        end
      end
    end

    if n_cpu > 0
      puts sprintf("avg: user=%.1f%% system=%.1f%% iowait=%.1f%% softirq=%.1f%%",
                   usr / n_cpu, sys / n_cpu, iow / n_cpu, soft / n_cpu)
    else
      puts "no samples"
    end

    puts "--- #{ip} network ---"
    net_raw = `ssh #{ssh_opts} root@#{ip} "cat /root/#{tag}-net.log" 2>/dev/null`
    net_raw.each_line do |line|
      cols = line.split
      next unless (cols[0] == "Average:" || cols[0] == "平均时间:") && cols[1] != "IFACE" && cols[1] != "lo"
      # sar -n DEV 标准输出中 cols[4] 为 rxkB/s，cols[5] 为 txkB/s
      puts sprintf("%s avg: rx=%.1fMB/s tx=%.1fMB/s", cols[1], cols[4].to_f / 1024.0, cols[5].to_f / 1024.0)
    end
  end
end
