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
my $interval = 5;      # Default refresh interval (seconds)
my $count    = -1;     # Default unlimited runs (-1 = infinite)
my $top_num  = 15;     # Default number of processes to show
my $output_file;       # Output file name

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
        print "  -c, -count   <times>    : Number of runs (default: unlimited)\n";
        print "  -n, -top     <number>   : Number of processes to show (default: 15)\n";
        print "  -o, -output  <filename> : Save output to file\n";
        print "  -h, -help               : Show this help message\n";
        print "\nExamples:\n";
        print "  perl oracle_top.pl -i 3 -c 10\n";
        print "  perl oracle_top.pl -i 5 -o /tmp/monitor.log\n";
        print "  perl oracle_top.pl -interval 2 -count 5 -top 20 -output monitor.txt\n";
        exit 0;
    }
}

# ==================== Database Connection ====================
my $dbh = DBI->connect("dbi:Oracle:", "sys", "oracle", {
    ora_session_mode => 2  # 2 means SYSDBA
}) or die "Connection failed: $DBI::errstr";

# ==================== Open Output File ====================
my $file_handle;
if ($output_file) {
    open($file_handle, '>>', $output_file) or die "Cannot open output file '$output_file': $!";
    print "Output will be saved to: $output_file\n";
}

print "Oracle Process Monitor - System Process to Database Session Mapping\n";
printf "Configuration: Interval: %ds, Show top: %d processes, %s\n",
       $interval, $top_num,
       $count > 0 ? "Run $count times then exit" : "Run indefinitely";
print "Press Ctrl+C to exit at any time\n\n";

# ==================== Main Loop ====================
my $run_count = 0;

while (!$exit_flag) {
    $run_count++;

    # Check if reached the specified run count
    if ($count > 0 && $run_count > $count) {
        print "\nReached specified run count ($count times), exiting.\n";
        $exit_flag = 1;
        last;
    }

    system('clear');

    # Get current timestamp
    my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
    $year += 1900;
    $mon += 1;
    my $timestamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);

    print "=" x 90 . "\n";
    print "TIMESTAMP: $timestamp | Run #$run_count";
    if ($count > 0) {
        print " (of $count total)";
    }
    print "\n";
    print "=" x 90 . "\n";

    # Get TOP processes (sorted by CPU)
    my %pids;
    open my $PS, '-|', "ps -eo pid,pcpu,pmem,comm --no-headers --sort=-pcpu | head -$top_num" or die $!;
    while (<$PS>) {
        my @fields = split;
        next unless @fields >= 4;
        my ($pid, $cpu, $mem, $cmd) = @fields;
        $pids{$pid} = { cpu => $cpu, mem => $mem, cmd => $cmd } if $pid =~ /^\d+$/;
    }
    close $PS;

    # Query Oracle session information
    my @pids = keys %pids;
    my %sessions;

    if (@pids) {
        my $placeholders = join(',', @pids);
        my $sql = "SELECT p.spid, s.sid, s.serial#, s.username, s.sql_id
                   FROM v\$session s, v\$process p
                   WHERE s.paddr = p.addr AND p.spid IN ($placeholders)";

        eval {
            my $sth = $dbh->prepare($sql);
            $sth->execute();

            while (my $row = $sth->fetchrow_hashref()) {
                $sessions{$row->{SPID}} = {
                    sid     => $row->{SID},
                    serial  => $row->{'SERIAL#'},
                    user    => $row->{USERNAME},
                    sql_id  => $row->{SQL_ID}
                };
            }
        };
        if ($@) {
            print "Database query error: $@\n";
        }
    }

    # Build output string
    my $output_string = "";

    # Add header to output string
    $output_string .= sprintf("%-8s %-6s %-6s %-20s | %-12s %-8s %-12s %-15s\n",
           'PID', 'CPU%', '%MEM', 'COMMAND',
           'ora-USER', 'ora-SID', 'ora-serial#', 'ora-sql_id');
    $output_string .= "-" x 90 . "\n";

    # Add data rows to output string
    my $display_count = 0;
    foreach my $pid (sort { $pids{$b}{cpu} <=> $pids{$a}{cpu} } keys %pids) {
        last if $display_count++ >= $top_num;

        my $proc = $pids{$pid};
        my $sess = $sessions{$pid};

        # System process information
        $output_string .= sprintf("%-8s %-6s %-6s %-20s | ",
               $pid,
               $proc->{cpu},
               $proc->{mem},
               substr($proc->{cmd} || 'N/A', 0, 20));

        # Oracle session information
        if ($sess) {
            $output_string .= sprintf("%-12s %-8s %-12s %-15s\n",
                   substr($sess->{user} || 'N/A', 0, 12),
                   $sess->{sid} || 'N/A',
                   $sess->{serial} || 'N/A',
                   $sess->{sql_id} || 'N/A');
        } else {
            $output_string .= sprintf("%-12s %-8s %-12s %-15s\n",
                   'N/A', 'N/A', 'N/A', 'N/A');
        }
    }

    # Add statistics to output string
    my $session_count = scalar keys %sessions;
    $output_string .= "=" x 90 . "\n";
    $output_string .= sprintf("Stats: Total processes: %d, Oracle sessions: %d\n",
           scalar(@pids), $session_count);

    # Print to screen
    print $output_string;
    print "Next refresh: in $interval seconds\n\n";

    # Save to file if output file is specified
    if ($output_file) {
        # Add timestamp and separator for file output
        my $file_output = "\n" . "=" x 90 . "\n";
        $file_output .= "TIMESTAMP: $timestamp | Run #$run_count";
        $file_output .= " (of $count total)" if $count > 0;
        $file_output .= "\n";
        $file_output .= "=" x 90 . "\n";
        $file_output .= $output_string;
        $file_output .= "Next refresh: in $interval seconds\n";

        print $file_handle $file_output;
    }

    # If exit flag is set, exit immediately
    last if $exit_flag;

    # Check if reached specified run count
    if ($count > 0 && $run_count >= $count) {
        last;
    }

    # Improved wait method: wait 1 second at a time, checking exit flag
    for (my $i = 0; $i < $interval && !$exit_flag; $i++) {
        sleep 1;
    }
}

# ==================== Cleanup and Exit ====================
$dbh->disconnect() if $dbh;
close($file_handle) if $output_file;
print "Monitoring program has exited.\n";
exit 0;
