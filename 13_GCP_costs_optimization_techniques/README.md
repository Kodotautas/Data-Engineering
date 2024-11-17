# GCP Costs Optimization Techniques

##### 1. Implement budgets and alerts
   - Set up billing budgets to monitor and control costs
   - Configure budget amount based on actual or forecasted costs
   - Set budget alerts at different thresholds (e.g. 50%, 75%, 90%, 100%)
   - Alerts can be sent via email notifications to billing admins and specified recipients
   - Can create multiple budgets for different projects/folders/billing accounts
   - Monitor spending trends and get notified before exceeding budget limits
   - Budgets can be set for specific projects, products or labels

   ![GCP Billing Budget Alert Configuration showing options to set budget amount, budget alerts thresholds, and notification recipients](images/budgets_alerts.png)
   *Image source: [Medium article](https://medium.com/google-cloud/how-to-set-up-budget-alerts-in-google-cloud-platform-gcp-94128044834f)*
   
   *Figure 1: GCP Billing Budget Alert Configuration example for Slack notifications*

   [Google Cloud Billing documentation](https://cloud.google.com/billing/docs/how-to/budgets)

##### 2. Quota and limits
   - Quotas protect GCP from being overwhelmed by sudden spikes in usage
   - Default quotas are automatically set for each project
   - Types of quotas:
     - Rate quotas: Limit API requests per time period
     - Allocation quotas: Limit number of resources you can have
     - Regional quotas: Limit resources in specific regions
   - Monitor quota usage in Cloud Console under IAM & Admin > Quotas
   - Request quota increases if needed:
     - Go to Quotas page in Console
     - Select the quota you want to increase
     - Click "Edit Quotas" and fill out request form
     - Google will review and approve/deny request
   - Best practices:
     - Regularly monitor quota usage
     - Plan ahead for quota increases (takes 2-3 business days)
     - Set up alerts for quota thresholds
     - Consider quotas during application design
     - Document quota requirements for your projects

   [Google Cloud Quotas documentation](https://cloud.google.com/compute/quotas-limits)


##### 3. Preemptible instances & spot instances
   - Preemptible VMs are cheaper instances that can be terminated at any time
   - Cost up to 80% less than regular instances
   - Maximum runtime of 24 hours
   - No live migration or automatic restart
   - Spot VMs are similar but with flexible pricing based on supply/demand
   - Best practices:
     - Use for fault-tolerant, stateless workloads
     - Design applications to handle interruptions
     - Save work frequently and checkpoint data
     - Use managed instance groups for automatic recreation
     - Monitor preemption notices (30 second warning)
   - Good use cases:
     - Batch processing jobs
     - Data analysis
     - CI/CD pipelines
     - Testing and development
   - Configure through Console or gcloud CLI:
     - Select preemptible/spot option when creating VM
     - Set maximum price for spot instances
     - Use with instance templates for scaling

   [Google Cloud Preemptible VMs documentation](https://cloud.google.com/compute/docs/instances/preemptible)
   [Google Cloud Spot VMs documentation](https://cloud.google.com/compute/docs/instances/spot)

   
##### 4. Committed usage discounts (CUD)
   - Discounted pricing for committing to use minimum level of resources
   - Available for Compute Engine, Cloud SQL, Cloud Spanner, and more
   - Commitment periods: 1 year or 3 years
   - Higher discounts for longer commitments (up to 70% off)
   - Types of commitments:
     - Resource-based: Specific machine types in specific regions
     - Spend-based: Dollar amount commitment, more flexible
   - Benefits:
     - Significant cost savings for predictable workloads
     - Can be shared across projects in same billing account
     - Automatic application of discounts
   - Best practices:
     - Analyze usage patterns before committing
     - Start with 1-year commitments to evaluate
     - Consider mix of CUD and on-demand for flexibility
     - Monitor commitment utilization
     - Use spend-based for varying workloads

   [Google Cloud CUD documentation](https://cloud.google.com/compute/docs/instances/signing-up-committed-use-discounts)

5. Sustained use discounts
6. Billing & Export
7. Organization policies
8. Schedule VM instances - with cloud scheduler & cloud functions
9. Schedule VM - auto start/stop
10. Life cycle policy in Google Cloud Storage
11. Identify unused disks and IP addresses
12. Custom virtual machine instances
13. Avoid dulicated data in GCS buckets
14. Region selection for costs optimization and performance
15. Bigquery optimization: 
    - avoid select *, 
    - use caching, 
    - turn on history based optimization feature
    - expiration for tables, views
    - use of partitions
    - use of clustering
    - flex slots and pricing
    - delete vs truncate
    - limit query number of bytes processed per query
16. On demand vs provision capacity
17. Egress pricing
18. Cloud monitoring for resource utilization
19. Cloud Shell - free machine for development
20. Different servces availability in regions: regional, zonal, multi-regional
21. Select disk type: HHD, SSD
22. GCP Recommendations for cost optimization
23. Schedule Dataproc cluster