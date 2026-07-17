#!/usr/bin/perl

use strict;
use warnings;
use DBI;

# ==================== Signal Handling ====================
my $exit_flag = 0;
$SIG{INT} = sub {
    $exit_flag = 1;
    print "\n\nInterrupt signal received, exiting...\n";
};

# ==================== Command Line Arguments ====================
my $interval = 5;      # 刷新间隔 (秒)
my $count    = -1;     # 执行次数 (-1 为无限)
my $top_num  = 15;     # 显示的 Top 进程数
my $output_file;       # 输出文件名

for (my $i = 0; $i < @ARGV; $i++) {
    if ($ARGV[$i] eq '-i' || $ARGV[$i] eq '-interval') {
        $interval = $ARGV[++$i] if $i+1 < @ARGV;
    }
    elsif ($ARGV[$i] eq '-c' || $ARGV[$i] eq '-count') {
        $count = $ARGV[++$i] if $i+1 < @ARGV;
    }
    elsif ($ARGV[$i] eq '-n' || $ARGV[$i] eq '-top') {
        $top_num = $ARGV[++$i] if $i+1 < @ARGV;
    }
    elsif ($ARGV[$i] eq '-o' || $ARGV[$i] eq '-output') {
        $output_file = $ARGV[++$i] if $i+1 < @ARGV;
    }
    elsif ($ARGV[$i] eq '-h' || $ARGV[$i] eq '-help') {
        print "Oracle TOP Monitor Usage:\n";
        print "  -i, -interval <seconds> : Refresh interval (default: 5)\n";
        print "  -c, -count   <times>     : Number of runs (default: unlimited)\n";
        print "  -n, -top     <number>    : Number of processes to show (default: 15)\n";
        print "  -o, -output  <filename>  : Save output to file\n";
        print "  -h, -help                : Show this help message\n";
        exit 0;
    }
}

# ==================== Database Connection Function ====================
sub connect_db {
    my $dbh = DBI->connect("dbi:Oracle:", "sys", "oracle", {
        ora_session_mode => 2,  # SYSDBA
        PrintError       => 0,
        RaiseError       => 1,  
        AutoCommit       => 1,
    });
    return $dbh;
}

my $dbh;
eval { $dbh = connect_db(); };
if ($@) {
    die "Initial database connection failed: $@\n";
}

# ==================== Statement Cache Mechanism ====================
# 核心优化：动态占位符句柄缓存，既避开硬解析，又避开了 CONNECT BY 的低效
my %STH_CACHE;
sub get_cached_sth {
    my ($dbh_ptr, $num_params) = @_;
    return $STH_CACHE{$num_params} if exists $STH_CACHE{$num_params};

    # 根据传入的 PID 数量动态生成 (?,?,?)
    my $placeholders = join(',', map { '?' } (1..$num_params));
    
    my $base_sql = qq{
        SELECT p.spid, s.sid, s.serial#, s.username, s.status, s.event, s.sql_id, s.last_call_et
        FROM v\$session s
        JOIN v\$process p ON s.paddr = p.addr
        WHERE p.spid IN ($placeholders)
    };
    
    my $new_sth = $dbh_ptr->prepare($base_sql);
    $STH_CACHE{$num_params} = $new_sth;
    return $new_sth;
}

# 清空句柄缓存（用于断线重连后）
sub clear_sth_cache {
    foreach my $cached_sth (values %STH_CACHE) {
        eval { $cached_sth->finish(); };
    }
    %STH_CACHE = ();
}

# ==================== Open Output File ====================
my $file_handle;
if ($output_file) {
    open($file_handle, '>>', $output_file) or die "Cannot open output file '$output_file': $!";
}

print "Oracle Process Monitor - Ultimate High-Performance Edition (v4)\n";
print "Press Ctrl+C to exit at any time\n\n";

# ==================== Main Loop ====================
my $run_count = 0;
my $total_width = 135; 

while (!$exit_flag) {
    $run_count++;

    if ($count > 0 && $run_count > $count) {
        print "\nReached specified run count ($count times), exiting.\n";
        $exit_flag = 1;
        last;
    }

    # 断线重连机制
    if (!$dbh || !$dbh->ping) {
        print "Database connection lost. Trying to reconnect...\n";
        clear_sth_cache();
        eval { $dbh = connect_db(); };
        if ($@) {
            print "Reconnect failed: $@. Will retry next interval.\n";
            sleep $interval;
            next;
        }
    }

    system('clear');

    my ($sec, $min, $hour, $mday, $mon, $year) = localtime(time);
    $year += 1900; $mon += 1;
    my $timestamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);

    print "=" x $total_width . "\n";
    print "TIMESTAMP: $timestamp | Run #$run_count";
    print " (of $count total)" if $count > 0;
    print "\n";
    print "=" x $total_width . "\n";

    # 从系统抓取 Top 进程
    my %pids;
    open my $PS, '-|', "ps -eo pid,pcpu,pmem,comm --no-headers --sort=-pcpu | head -$top_num" or die $!;
    while (<$PS>) {
        $_ =~ s/^\s+//; # 去除行首空格
        my @fields = split(/\s+/, $_);
        next unless @fields >= 4;
        my ($pid, $cpu, $mem, $cmd) = @fields;
        $pids{$pid} = { cpu => $cpu, mem => $mem, cmd => $cmd } if $pid =~ /^\d+$/;
    }
    close $PS;

    my @pids = keys %pids;
    my %sessions;

    if (@pids) {
        eval {
            # 获取对应参数数量的缓存句柄
            my $current_sth = get_cached_sth($dbh, scalar(@pids));
            $current_sth->execute(@pids);

            while (my @row = $current_sth->fetchrow_array()) {
                my $spid        = $row[0];
                $sessions{$spid} = {
                    sid         => $row[1],
                    serial      => $row[2],
                    user        => $row[3],
                    status      => $row[4],
                    event       => $row[5],
                    sql_id      => $row[6],
                    elapsed_sec => $row[7]
                };
            }
        };
        if ($@) {
            print "Database query error: $@\n";
            # 如果是内部句柄失效引起的报错，强制清理缓存以便下次重建
            clear_sth_cache();
        }
    }

    # 格式化组装输出字符串
    my $output_string = "";
    $output_string .= sprintf("%-8s %-5s %-5s %-16s | %-12s %-6s %-7s %-8s %-11s %-14s %-25s\n",
           'PID', 'CPU%', '%MEM', 'COMMAND',
           'ora-USER', 'SID', 'SERIAL#', 'STATUS', 'ELAPSED_S', 'SQL_ID', 'CURRENT EVENT');
    $output_string .= "-" x $total_width . "\n";

    my $display_count = 0;
    foreach my $pid (sort { $pids{$b}{cpu} <=> $pids{$a}{cpu} } keys %pids) {
        last if $display_count++ >= $top_num;

        my $proc = $pids{$pid};
        my $sess = $sessions{$pid};
        my $cmd  = $proc->{cmd} || 'N/A';

        $output_string .= sprintf("%-8s %-5s %-5s %-16s | ",
               $pid, $proc->{cpu}, $proc->{mem}, substr($cmd, 0, 16));

        if ($sess) {
            # 正常映射到会话
            $output_string .= sprintf("%-12s %-6s %-7s %-8s %-11s %-14s %-25s\n",
                   substr($sess->{user} || 'SYS_BG', 0, 12),
                   $sess->{sid}    || 'N/A',
                   $sess->{serial} || 'N/A',
                   $sess->{status} || 'N/A',
                   $sess->{elapsed_sec} // 0,
                   $sess->{sql_id} || 'NONE',
                   substr($sess->{event}  || 'SQL*Net message', 0, 25));
        } else {
            # 未在 v$session 中关联上的进程处理
            if ($cmd =~ /ora_([a-z0-9]+)_/i) {
                # 属于 Oracle 后台进程但无活跃会话映射
                $output_string .= sprintf("%-12s %-6s %-7s %-8s %-11s %-14s %-25s\n",
                       "ORA_BG [$1]", 'N/A', 'N/A', 'ACTIVE', '0', 'NONE', 'No Active Event');
            } else {
                # 纯粹的非 Oracle 进程
                $output_string .= sprintf("%-12s %-6s %-7s %-8s %-11s %-14s %-25s\n",
                       'NON_ORACLE', 'N/A', 'N/A', 'N/A', '-', 'N/A', 'N/A');
            }
        }
    }

    my $session_count = scalar keys %sessions;
    $output_string .= "=" x $total_width . "\n";
    $output_string .= sprintf("Stats: Total active OS processes: %d, Mapped Oracle sessions: %d\n",
           scalar(@pids), $session_count);

    print $output_string;
    print "Next refresh: in $interval seconds\n\n";

    if ($output_file) {
        my $file_output = "\n" . "=" x $total_width . "\n"
                        . "TIMESTAMP: $timestamp | Run #$run_count"
                        . ($count > 0 ? " (of $count total)" : "") . "\n"
                        . "=" x $total_width . "\n"
                        . $output_string
                        . "Next refresh: in $interval seconds\n";
        print $file_handle $file_output;
    }

    last if $exit_flag;

    # 精确平滑的 Sleep 控制
    for (my $i = 0; $i < $interval && !$exit_flag; $i++) {
        sleep 1;
    }
}

# ==================== Cleanup and Exit ====================
clear_sth_cache();
if ($dbh) {
    eval { $dbh->disconnect(); };
}
close($file_handle) if $output_file;
print "Monitoring program has exited cleanly.\n";
exit 0;
