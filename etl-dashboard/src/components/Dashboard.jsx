import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { 
  Play, 
  Download, 
  FileText, 
  BarChart3, 
  Database, 
  Settings, 
  Clock,
  CheckCircle,
  AlertCircle,
  FileSpreadsheet,
  TrendingUp,
  Users,
  Calendar
} from 'lucide-react';
import { motion } from 'framer-motion';
import '../App.css';

const Dashboard = () => {
  const [pipelineStatus, setPipelineStatus] = useState('idle');
  const [progress, setProgress] = useState(0);
  const [lastRunTime, setLastRunTime] = useState(null);
  const [stats, setStats] = useState({
    totalFiles: 0,
    totalRows: 0,
    lastRunDuration: 0,
    successfulRuns: 12
  });

  // Simulate pipeline execution
  const runPipeline = async () => {
    setPipelineStatus('running');
    setProgress(0);
    
    // Simulate progress
    const progressSteps = [
      { step: 'Extracting data...', progress: 25 },
      { step: 'Transforming data...', progress: 50 },
      { step: 'Loading data...', progress: 75 },
      { step: 'Generating reports...', progress: 100 }
    ];

    for (const { step, progress: stepProgress } of progressSteps) {
      await new Promise(resolve => setTimeout(resolve, 1500));
      setProgress(stepProgress);
    }

    setPipelineStatus('completed');
    setLastRunTime(new Date());
    setStats(prev => ({
      ...prev,
      totalFiles: Math.floor(Math.random() * 50) + 10,
      totalRows: Math.floor(Math.random() * 10000) + 1000,
      lastRunDuration: Math.floor(Math.random() * 120) + 30,
      successfulRuns: prev.successfulRuns + 1
    }));
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return 'bg-blue-500';
      case 'completed': return 'bg-green-500';
      case 'error': return 'bg-red-500';
      default: return 'bg-gray-500';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'running': return <Clock className="h-4 w-4" />;
      case 'completed': return <CheckCircle className="h-4 w-4" />;
      case 'error': return <AlertCircle className="h-4 w-4" />;
      default: return <Database className="h-4 w-4" />;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-900 dark:to-slate-800">
      <div className="container mx-auto p-6">
        {/* Header */}
        <motion.div 
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          className="mb-8"
        >
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-4xl font-bold text-slate-900 dark:text-slate-100 mb-2">
                ETL Reporting Dashboard
              </h1>
              <p className="text-slate-600 dark:text-slate-400">
                Automated data processing and report generation
              </p>
            </div>
            <div className="flex items-center gap-4">
              <Badge variant="outline" className="flex items-center gap-2">
                {getStatusIcon(pipelineStatus)}
                {pipelineStatus.charAt(0).toUpperCase() + pipelineStatus.slice(1)}
              </Badge>
              <Button 
                onClick={runPipeline}
                disabled={pipelineStatus === 'running'}
                className="flex items-center gap-2"
              >
                <Play className="h-4 w-4" />
                Run Pipeline
              </Button>
            </div>
          </div>
        </motion.div>

        {/* Progress Bar */}
        {pipelineStatus === 'running' && (
          <motion.div 
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            className="mb-6"
          >
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium">Pipeline Progress</span>
                  <span className="text-sm text-slate-600">{progress}%</span>
                </div>
                <Progress value={progress} className="h-2" />
              </CardContent>
            </Card>
          </motion.div>
        )}

        {/* Stats Cards */}
        <motion.div 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8"
        >
          <Card className="hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-slate-600 dark:text-slate-400">Files Processed</p>
                  <p className="text-3xl font-bold text-slate-900 dark:text-slate-100">{stats.totalFiles}</p>
                </div>
                <FileSpreadsheet className="h-8 w-8 text-blue-500" />
              </div>
            </CardContent>
          </Card>

          <Card className="hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-slate-600 dark:text-slate-400">Total Rows</p>
                  <p className="text-3xl font-bold text-slate-900 dark:text-slate-100">{stats.totalRows.toLocaleString()}</p>
                </div>
                <Database className="h-8 w-8 text-green-500" />
              </div>
            </CardContent>
          </Card>

          <Card className="hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-slate-600 dark:text-slate-400">Last Run Duration</p>
                  <p className="text-3xl font-bold text-slate-900 dark:text-slate-100">{stats.lastRunDuration}s</p>
                </div>
                <Clock className="h-8 w-8 text-orange-500" />
              </div>
            </CardContent>
          </Card>

          <Card className="hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-slate-600 dark:text-slate-400">Successful Runs</p>
                  <p className="text-3xl font-bold text-slate-900 dark:text-slate-100">{stats.successfulRuns}</p>
                </div>
                <TrendingUp className="h-8 w-8 text-purple-500" />
              </div>
            </CardContent>
          </Card>
        </motion.div>

        {/* Main Content */}
        <motion.div 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          <Tabs defaultValue="overview" className="space-y-6">
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="reports">Reports</TabsTrigger>
              <TabsTrigger value="data">Data Sources</TabsTrigger>
              <TabsTrigger value="settings">Settings</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="space-y-6">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Recent Activity */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Clock className="h-5 w-5" />
                      Recent Activity
                    </CardTitle>
                    <CardDescription>Latest pipeline executions and system events</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {[
                        { time: '2 minutes ago', action: 'Pipeline completed successfully', status: 'success' },
                        { time: '1 hour ago', action: 'Data transformation completed', status: 'success' },
                        { time: '3 hours ago', action: 'New files detected in input folder', status: 'info' },
                        { time: '1 day ago', action: 'Weekly report generated', status: 'success' }
                      ].map((activity, index) => (
                        <div key={index} className="flex items-center gap-3 p-3 rounded-lg bg-slate-50 dark:bg-slate-800">
                          <div className={`w-2 h-2 rounded-full ${
                            activity.status === 'success' ? 'bg-green-500' : 
                            activity.status === 'error' ? 'bg-red-500' : 'bg-blue-500'
                          }`} />
                          <div className="flex-1">
                            <p className="text-sm font-medium">{activity.action}</p>
                            <p className="text-xs text-slate-500">{activity.time}</p>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>

                {/* System Health */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <BarChart3 className="h-5 w-5" />
                      System Health
                    </CardTitle>
                    <CardDescription>Current system status and performance metrics</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium">Data Processing</span>
                        <Badge variant="outline" className="text-green-600 border-green-200">Healthy</Badge>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium">Storage Usage</span>
                        <span className="text-sm text-slate-600">2.3 GB / 10 GB</span>
                      </div>
                      <Progress value={23} className="h-2" />
                      
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium">Memory Usage</span>
                        <span className="text-sm text-slate-600">45%</span>
                      </div>
                      <Progress value={45} className="h-2" />
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Last Run Summary */}
              {lastRunTime && (
                <Card>
                  <CardHeader>
                    <CardTitle>Last Pipeline Execution</CardTitle>
                    <CardDescription>
                      Completed on {lastRunTime.toLocaleString()}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                      <div className="text-center">
                        <div className="text-2xl font-bold text-blue-600">{stats.totalFiles}</div>
                        <div className="text-sm text-slate-600">Files Processed</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-green-600">{stats.totalRows.toLocaleString()}</div>
                        <div className="text-sm text-slate-600">Rows Processed</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-orange-600">{stats.lastRunDuration}s</div>
                        <div className="text-sm text-slate-600">Execution Time</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            <TabsContent value="reports" className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <FileText className="h-5 w-5" />
                    Generated Reports
                  </CardTitle>
                  <CardDescription>Download and view your automated reports</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {[
                      { name: 'Sales Summary Report', type: 'Excel', size: '2.3 MB', date: '2024-01-15' },
                      { name: 'Customer Analytics', type: 'CSV', size: '1.8 MB', date: '2024-01-15' },
                      { name: 'Financial Overview', type: 'Excel', size: '3.1 MB', date: '2024-01-14' },
                      { name: 'Inventory Report', type: 'CSV', size: '945 KB', date: '2024-01-14' },
                      { name: 'Performance Metrics', type: 'Excel', size: '1.2 MB', date: '2024-01-13' },
                      { name: 'Data Quality Report', type: 'PDF', size: '567 KB', date: '2024-01-13' }
                    ].map((report, index) => (
                      <Card key={index} className="hover:shadow-md transition-shadow">
                        <CardContent className="p-4">
                          <div className="flex items-start justify-between mb-2">
                            <div className="flex-1">
                              <h4 className="font-medium text-sm">{report.name}</h4>
                              <p className="text-xs text-slate-500">{report.date}</p>
                            </div>
                            <Badge variant="secondary" className="text-xs">{report.type}</Badge>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="text-xs text-slate-500">{report.size}</span>
                            <Button size="sm" variant="outline">
                              <Download className="h-3 w-3 mr-1" />
                              Download
                            </Button>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="data" className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Database className="h-5 w-5" />
                    Data Sources
                  </CardTitle>
                  <CardDescription>Manage your input data sources and connections</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    {[
                      { name: 'Sales Data Folder', path: '/data/input/sales/', files: 23, status: 'active' },
                      { name: 'Customer Database', path: '/data/input/customers/', files: 12, status: 'active' },
                      { name: 'Inventory Files', path: '/data/input/inventory/', files: 8, status: 'active' },
                      { name: 'Financial Reports', path: '/data/input/finance/', files: 15, status: 'inactive' }
                    ].map((source, index) => (
                      <div key={index} className="flex items-center justify-between p-4 border rounded-lg">
                        <div className="flex items-center gap-3">
                          <div className={`w-3 h-3 rounded-full ${
                            source.status === 'active' ? 'bg-green-500' : 'bg-gray-400'
                          }`} />
                          <div>
                            <h4 className="font-medium">{source.name}</h4>
                            <p className="text-sm text-slate-500">{source.path}</p>
                          </div>
                        </div>
                        <div className="flex items-center gap-4">
                          <span className="text-sm text-slate-600">{source.files} files</span>
                          <Button size="sm" variant="outline">
                            <Settings className="h-3 w-3 mr-1" />
                            Configure
                          </Button>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="settings" className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Settings className="h-5 w-5" />
                    Pipeline Configuration
                  </CardTitle>
                  <CardDescription>Configure your ETL pipeline settings and automation</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-6">
                    <div>
                      <h4 className="font-medium mb-2">Scheduling</h4>
                      <div className="space-y-2">
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" defaultChecked />
                          <span className="text-sm">Run automatically every day at 6:00 AM</span>
                        </label>
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" />
                          <span className="text-sm">Run when new files are detected</span>
                        </label>
                      </div>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Output Formats</h4>
                      <div className="space-y-2">
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" defaultChecked />
                          <span className="text-sm">Excel (.xlsx) with styling</span>
                        </label>
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" defaultChecked />
                          <span className="text-sm">CSV files</span>
                        </label>
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" />
                          <span className="text-sm">PDF reports</span>
                        </label>
                      </div>
                    </div>

                    <div>
                      <h4 className="font-medium mb-2">Notifications</h4>
                      <div className="space-y-2">
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" defaultChecked />
                          <span className="text-sm">Email notifications on completion</span>
                        </label>
                        <label className="flex items-center gap-2">
                          <input type="checkbox" className="rounded" />
                          <span className="text-sm">Slack notifications</span>
                        </label>
                      </div>
                    </div>

                    <div className="pt-4">
                      <Button>Save Configuration</Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </motion.div>
      </div>
    </div>
  );
};

export default Dashboard;

