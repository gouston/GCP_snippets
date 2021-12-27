package access_control;

/*
 Copyright 2022 Mike Houston birockstar.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   */

//import com.google.auth.appengine.AppEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RetryOption;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.annotation.ServletSecurity;
import javax.servlet.annotation.HttpConstraint;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.threeten.bp.Duration;

@SuppressWarnings("serial")
@WebServlet(name = "ACL", value = "/acl")
@ServletSecurity(value = @HttpConstraint(rolesAllowed = { "admin" }))

public class ACL extends HttpServlet {

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

        // String d = LocalDateTime.now().format(DateTimeFormatter.ofPattern("_yyyyMMdd_kkmmss"));
        String projectId = "";
        String datasetName = "";
        String federatedTableName = "ACL";
        String tableName = "tbl_ACL";
        String destDatasetName = datasetName;
        boolean allowLargeResults = true;
        String businessKeyColumn = "";

        // insert new permissions
        String insertquery = "SELECT Distinct a." + businessKeyColumn
                + ", trim(a.UserAccount) as UserAccount, CURRENT_TIMESTAMP() as InsertDate \n" + "FROM `" + projectId
                + "." + datasetName + "." + federatedTableName + "` a\n" + "where not exists (\n" + "select 1\n"
                + "from `" + projectId + "." + datasetName + "." + tableName + "` b\n" + "where a." + businessKeyColumn
                + " = b." + businessKeyColumn + " and trim(a.UserAccount) = b.UserAccount\n" + ")";

        try {
            runQueryPermanentTable(projectId, insertquery, destDatasetName, tableName, allowLargeResults, resp);
        } catch (InterruptedException e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Interrupted while running BigQuery job.");
        } catch (TimeoutException e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Timeout while running BigQuery job.");
        }

        // delete query
        String deletequery = "delete from `" + projectId + "." + datasetName + "." + tableName + "` a \n"
                + "where not exists (\n" + "select 1 from `b" + projectId + "." + datasetName + "." + federatedTableName
                + "` b\n" + "where a." + businessKeyColumn + " = b." + businessKeyColumn
                + " and trim(a.UserAccount) = b.UserAccount);";

        try {
            runStandardSqlQuery(projectId, deletequery, resp);
        } catch (InterruptedException e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Interrupted while running BigQuery job.");
        } catch (TimeoutException e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Timeout while running BigQuery job.");
        }

        resp.setStatus(HttpServletResponse.SC_OK);

    }

    public static void runQueryPermanentTable(String projectId, String queryString, String destinationDataset, String destinationTable,
            boolean allowLargeResults, HttpServletResponse resp)
            throws TimeoutException, InterruptedException, IOException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString)
                // Save the results of the query to a permanent table. See:
                // https://cloud.google.com/bigquery/docs/writing-results#permanent-table
                .setDestinationTable(TableId.of(destinationDataset, destinationTable))
                // Allow results larger than the maximum response size.
                // If true, a destination table must be set. See:
                // https://cloud.google.com/bigquery/docs/writing-results#large-results
                .setAllowLargeResults(allowLargeResults).setUseLegacySql(false).setFlattenResults(true)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND).build();

        runQuery(projectId, queryConfig, resp);
    }

    public static void runStandardSqlQuery(String projectId, String queryString, HttpServletResponse resp)
            throws TimeoutException, InterruptedException, IOException {

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString)
                // To use standard SQL syntax, set useLegacySql to false. See:
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql
                .setUseLegacySql(false).build();

        runQuery(projectId, queryConfig, resp);
    }

    public static void runQuery(String projectId, QueryJobConfiguration queryConfig, HttpServletResponse resp)
            throws TimeoutException, InterruptedException, IOException {

        // BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        ArrayList<String> scopes = new ArrayList<String>();
        scopes.add("https://www.googleapis.com/auth/drive.readonly");
        scopes.add("https://www.googleapis.com/auth/cloud-platform");

        GoogleCredentials credentials;
        
        credentials = GoogleCredentials.getApplicationDefault();
        
        credentials = credentials.createScoped(scopes);

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build()
                .getService();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        PrintWriter out = resp.getWriter();
        resp.setContentType("text/plain");
        String i = queryJob.getStatistics().getEndTime().toString();
        out.println("The query completed at: " + i);

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
    }

    public static void copy(String sourceDatasetName, String sourceTableName, String destDatasetName,
            String destTableName, HttpServletResponse resp) throws TimeoutException, InterruptedException, IOException {

        PrintWriter out = resp.getWriter();
        resp.setContentType("text/plain");

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        TableId tableId = TableId.of(sourceDatasetName, sourceTableName);
        Table bqtable = bigquery.getTable(tableId);
        if (bqtable == null) {
            out.println("Oops! The table: " + bqtable + " does not exist! :-( ");
        }

        // [START copy]
        Job job = bqtable.copy(destDatasetName, destTableName);

        // Wait for the job to complete.
        try {
            Job completedJob = job.waitFor(RetryOption.initialRetryDelay(Duration.ofSeconds(3)),
                    RetryOption.totalTimeout(Duration.ofMinutes(15)));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                // Job completed successfully
                out.println("The copy job succeeded");
            } else {
                // Handle error case
                throw new RuntimeException(completedJob.getStatus().getError().toString());
            }
        } catch (InterruptedException e) {
            // Handle interrupted wait
            throw new RuntimeException(e.toString());
        }

        // [END copy]
    }

}
