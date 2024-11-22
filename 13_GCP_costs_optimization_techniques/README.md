# GCP Costs Optimization Techniques and best practices

##### 1. Implement budgets and alerts
   Best practices for implementing budgets and alerts:
   - Start with a conservative budget based on historical usage patterns
   - Set up multiple budget thresholds (50%, 75%, 90%, 100%) for early warnings
   - Configure alerts to notify both billing admins and project owners
   - Create separate budgets for each major project/department
   - Use labels consistently to track costs by application/environment
   - Review and adjust budgets quarterly based on actual spend
   - Enable programmatic notifications (e.g. Cloud Functions) for automated responses
   - Document budget allocation decisions and alert configurations
   - Set up dashboards to visualize spend against budgets
   - Consider seasonal variations when setting budget amounts

   ![GCP Billing Budget Alert Configuration showing options to set budget amount, budget alerts thresholds, and notification recipients](images/budgets_alerts.png)
   *Image source: [Medium article](https://medium.com/google-cloud/how-to-set-up-budget-alerts-in-google-cloud-platform-gcp-94128044834f)*
   
   *Figure 1: GCP Billing Budget Alert Configuration example for Slack notifications*

   [Google Cloud Billing documentation](https://cloud.google.com/billing/docs/how-to/budgets)

##### 2. Quota and limits
   - Best practices:
     - Regularly monitor quota usage
     - Plan ahead for quota increases (takes 2-3 business days)
     - Set up alerts for quota thresholds
     - Consider quotas during application design
     - Document quota requirements for your projects

   [Google Cloud Quotas documentation](https://cloud.google.com/compute/quotas-limits)


##### 3. Preemptible instances & spot instances
   - Best practices:
     - Use for fault-tolerant, stateless workloads
     - Design applications to handle interruptions
     - Save work frequently and checkpoint data
     - Use managed instance groups for automatic recreation
     - Monitor preemption notices (30 second warning)

   [Google Cloud Preemptible VMs documentation](https://cloud.google.com/compute/docs/instances/preemptible)
   [Google Cloud Spot VMs documentation](https://cloud.google.com/compute/docs/instances/spot)

   
##### 4. Committed usage discounts (CUD)
   - Best practices:
     - Analyze usage patterns before committing
     - Start with 1-year commitments to evaluate
     - Consider mix of CUD and on-demand for flexibility
     - Monitor commitment utilization
     - Use spend-based for varying workloads

   [Google Cloud CUD documentation](https://cloud.google.com/compute/docs/instances/signing-up-committed-use-discounts)

##### 5. Sustained use discounts
   - Best practices:
     - Analyze usage patterns before committing
     - Start with 1-year commitments to evaluate
     - Consider mix of CUD and on-demand for flexibility
     - Monitor commitment utilization
     - Use spend-based for varying workloads

   [Google Cloud SUD documentation](https://cloud.google.com/compute/docs/sustained-use-discounts)
   ![sustained use discounts](images/sustained.png)
   *Figure 2: Sustained use discounts example*

##### 6. Billing & Export
   - Best practices:
     - Use Cloud Billing API for programmatic access
     - Export data to BigQuery for analysis
     - Set up alerts and dashboards in BigQuery
     - Use Cloud Billing export to S3 for long-term storage

   [Google Cloud Billing Export documentation](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)

##### 7. Organization policies
   - Best practices:
     - Use organization policies to manage resource settings
     - Set policies for projects, folders, and billing accounts
     - Use policies to enforce settings like region, service restrictions, etc.
     - Review policies regularly to ensure compliance

   [Google Cloud Organization Policies documentation](https://cloud.google.com/resource-manager/docs/organization-policy/overview)

##### 8. Schedule VM instances - with cloud scheduler & cloud functions
   - Best practices:
     - Use Cloud Scheduler to trigger VM start/stop
     - Cloud Functions to handle VM management logic
     - Set up triggers based on time, API activity, or other events
     - Monitor execution logs and errors
     - Ensure functions have appropriate permissions

   [Google Cloud Cloud Scheduler documentation](https://cloud.google.com/scheduler/docs/start-and-stop-compute-engine-instances-on-a-schedule)

##### 9. Life cycle policy in Google Cloud Storage
    - Best practices:
      - use lifecycle policy to manage objects
      - set policies for different storage classes
      - consider storage class for different types of data
      - review policies regularly to ensure compliance

   [Google Cloud Life Cycle Policy documentation](https://cloud.google.com/storage/docs/lifecycle)

##### 10. Identify unused disks and IP addresses
    - Best practices:
      - use gcloud command to identify unused disks
      - set up alerts for unused resources
      - consider automation for periodic checks
      - review unused resources regularly

   [Google Cloud gcloud command documentation](https://cloud.google.com/compute/docs/viewing-and-applying-idle-resources-recommendations)

##### 11. Custom virtual machine instances
    - Best practices:
      - use custom machine types for specific workloads
      - consider vCPU and memory requirements
      - review machine types regularly
      - use Shielded VMs for enhanced security

   [Google Cloud Custom Machine Types documentation](https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type)

##### 12. Avoid dulicated data in GCS buckets
    - Best practices:
      - use naming conventions to avoid duplicates
      - consider versioning for important data
      - review bucket usage regularly

##### 13. Region selection for costs optimization and performance
    - Best practices:
      - choose regions based on data residency, performance, and compliance requirements
      - consider regional vs multi-regional options
      - use Cloud Console or gcloud to check service availability
      - review region-specific costs and performance characteristics

   [Google Cloud Region Selection documentation](https://cloud.google.com/solutions/best-practices-compute-engine-region-selection)

##### 14. Bigquery optimization: 
    - Best practices:
      - avoid select *, 
      - use caching, 
      - turn on history based optimization feature
      - expiration for tables, views
      - use of partitions
      - use of clustering
      - flex slots and pricing
      - delete vs truncate
      - limit query number of bytes processed per query

##### 15. On demand vs provision capacity
    - Best practices:
      - use on-demand for flexible workloads
      - consider provisioned capacity for predictable workloads
      - use Cloud Console or gcloud to check pricing and availability
      - review capacity options regularly

##### 16. Egress pricing
    - Best practices:
      - understand egress pricing model
      - use caching to reduce egress costs
      - consider regional vs multi-regional storage
      - use Cloud Console or gcloud to check egress costs
      - review egress usage and costs regularly

   [Google Cloud Egress Pricing documentation](https://cloud.google.com/vpc/network-pricing)

##### 17. Cloud monitoring for resource utilization
    - Best practices:
      - use Cloud Monitoring to track resource utilization
      - set up alerts for high resource usage
      - consider using Stackdriver for advanced monitoring
      - review monitoring data regularly

##### 18. Cloud Shell - free machine for development
    - Best practices:
      - use Cloud Shell for quick access to GCP resources
      - use Cloud Shell for development tasks
      - consider using Cloud Shell for interactive sessions
      - review Cloud Shell usage and costs regularly

##### 19. Different servces availability in regions: regional, zonal, multi-regional
    - Best practices:
      - understand availability options for different services
      - consider regional vs zonal services
      - use Cloud Console or gcloud to check service availability
      - review service availability and costs regularly

##### 20. Select disk type: HHD, SSD
    - Best practices:
      - consider HHD for cost-sensitive workloads
      - use SSD for high-performance, low-latency workloads
      - review disk types and costs regularly

   [Google Cloud Disk Types documentation](https://cloud.google.com/compute/docs/disks)

##### 21. GCP Recommendations for cost optimization
    - Best practices:
      - use GCP Recommendations for cost optimization
      - consider using Cloud Console or gcloud to check recommendations
      - review recommendations regularly

   [Google Cloud Recommendations documentation](https://cloud.google.com/recommendations)

##### 22. Schedule Dataproc cluster
    - Best practices:
      - use Cloud Scheduler to trigger Dataproc cluster
      - Cloud Functions to handle Dataproc cluster management logic
      - set up triggers based on time, API activity, or other events
      - monitor execution logs and errors
      - ensure functions have appropriate permissions

   [Google Cloud Cloud Scheduler documentation](https://cloud.google.com/dataproc/docs/tutorials/workflow-scheduler)