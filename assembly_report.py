#!/usr/bin/python

import os;
import math;

# os.chdir('data/postgres/linux.env')
os.chdir('data/mysql/linux.env')
# os.chdir('data/mongo/linux.env')

col_time = 0;
col_read_ops = 1
col_read_err = 2
col_write_ops = 3
col_write_err = 4


class ColumnData:
    def __init__(self, chart, title, data):
        self.chart = chart;
        self.title = title;
        self.data = data;
        self.sum = sum(self.data);
        self.avg = self.sum / len(self.data);
        self.sd = math.sqrt(sum([math.pow(x - self.avg, 2) for x in data]) / len(self.data));

    def aggregate(self, group_size):
        assert len(self.data) % group_size == 0
        agg_data = [0.0 for i in range(len(self.data) / group_size)]
        for i in range(len(self.data)):
            agg_data[i / group_size] += self.data[i]
        agg_column = ColumnData(self.chart, self.title + '_agg', [x / group_size for x in agg_data])
        agg_column.sum = self.sum
        agg_column.avg = self.avg
        agg_column.sd = self.sd
        return agg_column


class ChartData:
    def __init__(self, file):
        assert file.endswith('.csv')

        # read meta-data:
        for field in file[:-len('.csv')].split(','):
            [key, value] = field.split('=');
            setattr(self, key, int(value));

        #  read raw data:
        first_line = True;
        input_matrix = None
        for line in open(file, 'r'):
            line = line.strip();
            if line <> '':
                items = line.split(',')
                if first_line:
                    input_matrix = [[title.replace("'", '')] for title in items]
                    first_line = False;
                else:
                    values = [float(value) for value in items]
                    for i in range(len(values)):
                        input_matrix[i].append(values[i])
        self.columns = [ColumnData(self, input_column[0], input_column[1:]) for input_column in input_matrix]

        self.time_line = self.columns[0]

        self.read_th = self.r_lite + self.r_heavy;
        read_title = 'r%d_R%d' % (self.r_lite, self.r_heavy)
        self.read_ops = self.columns[1]
        self.read_ops.title = 'R_' + read_title
        self.read_err = self.columns[2]
        self.read_err.title = 'RE_' + read_title

        self.write_th = self.w_ins + self.w_up_tiny + self.w_up_wide;
        write_title = 'i%d_u%d_U%d' % (self.w_ins, self.w_up_tiny, self.w_up_wide)
        self.write_ops = self.columns[3]
        self.write_ops.title = 'W_' + write_title
        self.write_err = self.columns[4]
        self.write_err.title = 'WE_' + write_title


name_index = 0;


def draw_chart(columns, name='', notes=''):
    if name == '':
        global name_index;
        name_index += 1;
        name = 'chart_%s' % name_index
    id = 'chart_' + name;
    result = "";
    result += """
    function %s() {
        var data = google.visualization.arrayToDataTable([
    """ % id;
    result += '[%s],\n' % ', '.join(['"' + c.title + '"' for c in columns])
    for i in range(len(columns[0].data)):
        result += '[%s],\n' % (', '.join([str(c.data[i]) for c in columns]))

    result += """
    ]);

        var options = {
          title: '%s',
          //curveType: 'function',
          chartArea:{left:60,top:10,width:'65%%',height:'85%%'}
        };

        var chart = new google.visualization.LineChart(document.getElementById('%s'));
        chart.draw(data, options);
    }
    """ % (name, id);
    return id, result


charts = []


def draw_aggregated_chart(name, columns, read_from=0, read_to=0, write_from=0, write_to=0):
    read_chart = []
    for file_csv in os.listdir('.'):
        if file_csv.endswith('.csv'):
            items = file_csv.replace('=', '_').replace('.', '_').split('_');
            read_threads = int(items[4]);
            write_threads = int(items[6]);
            if read_from <= read_threads <= read_to and write_from <= write_threads <= write_to:
                chart = read_chart_data(file_csv);
                if len(read_chart) == 0:
                    read_chart = [[t] for t in extract_column(chart, col_time)];
                for column in columns:
                    column_data = extract_column(chart, column)
                    if sum(column_data[1:]) == 0.0:
                        continue;
                    read_chart = append_column(read_chart, column_data);
    return draw_chart(read_chart, name);


def meta_column(columns, title, metric):
    return ColumnData(None, title, [metric(c) for c in columns])

def render_group(time_line, group_list, meta_prefix, threads_metric):
    global c
    charts.append(draw_chart([time_line] + [c.write_ops for c in group_list]));
    charts.append(draw_chart([time_line.aggregate(10)] + [c.write_ops.aggregate(10) for c in group_list]));
    charts.append(draw_chart([
        meta_column([c.write_ops for c in group_list], meta_prefix + ' Threads', threads_metric),
        meta_column([c.write_ops for c in group_list], meta_prefix + ' ops avg', lambda c: c.avg),
        meta_column([c.write_ops for c in group_list], meta_prefix + ' ops sd', lambda c: c.sd),
    ]));


if True:
    chart_list = []
    for file_name in os.listdir('.'):
        if file_name.endswith('.csv'):
            chart_list.append(ChartData(file_name));

    chart_ins_list = [c for c in chart_list if c.w_ins > 0 and c.read_th==0]
    chart_up_tiny_list = [c for c in chart_list if c.w_up_tiny > 0 and c.read_th==0]
    chart_up_wide_list = [c for c in chart_list if c.w_up_wide > 0 and c.read_th==0]
    chart_r_lite_list = [c for c in chart_list if c.r_lite > 0 and c.write_th==0]
    chart_r_heavy_list = [c for c in chart_list if c.r_heavy > 0 and c.write_th==0]
    time_line = chart_list[0].time_line

    if len(chart_ins_list)>0:
        render_group(time_line, chart_ins_list, 'Write Ins', lambda c: c.chart.write_th)
    if len(chart_up_tiny_list)>0:
        render_group(time_line, chart_up_tiny_list, 'Write Up Tiny', lambda c: c.chart.write_th)
    if len(chart_up_wide_list)>0:
        render_group(time_line, chart_up_wide_list, 'Write Up Wide', lambda c: c.chart.write_th)

with open('report-all.html', 'w') as out:
    out.write("""<html>
    <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(function(){
    """);
    for id, renderer in charts:
        out.write("         %s();\n" % id);
    out.write("""    
      });
      """);
    for id, renderer in charts:
        out.write(renderer);

    out.write("""
    </script>
    </head>
    <body>
    """);

    for id, renderer in charts:
        out.write('     <div id="%s" style="width: 1200px; height: 400px;"></div>\n' % id)

    out.write("""
        </body>
</html>""");
