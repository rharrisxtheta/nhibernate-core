using System.Data;
using System.Reflection;
using System.Text;

using NHibernate.AdoNet.Util;
using System;

namespace NHibernate.AdoNet
{
    /// <summary>
    /// Batcher for iDB2 (DB2 on iSeries / AS400)
    /// </summary>
    /// <remarks>
    /// Batching is only supported on newer iDB2 client side driver versions (even though it's 
    /// supported on almost all versions of the iDB2 server).   So, this batcher detects whether
    /// the client is new enough to support batching and either enables or disables it on the fly.
    /// </remarks>
    public class DB2400BatchingBatcher : AbstractBatcher
    {
        #region Fields

        /// <summary>
        /// Tracks the user's assigned batch size.
        /// </summary>
        private int batchSize;

        /// <summary>
        /// Tracks the number of statements from the current batch.
        /// </summary>
        private int countOfStatementsInCurrentBatch;

        /// <summary>
        /// Tracks the command that we're using as the 'root of the batch'
        /// </summary>
        private IDbCommand currentBatch;

        /// <summary>
        /// Tracks the SQL we're logging.
        /// </summary>
        private StringBuilder currentBatchCommandsLog;

        /// <summary>
        /// Tracks whether we're running a version of the iDB2 driver that supports batching.
        /// </summary>
        private bool supportsBatching;

        /// <summary>
        /// Tracks the rows affected across all the commands that were combined.
        /// </summary>
        private int totalExpectedRowsAffected;

        #endregion Fields

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DB2400BatchingBatcher"/> class.
        /// </summary>
        /// <param name="connectionManager">
        /// The <see cref="ConnectionManager"/> owning this batcher.
        /// </param>
        /// <param name="interceptor">The interceptor.</param>
        public DB2400BatchingBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
            : base(connectionManager, interceptor)
        {
            this.batchSize = Factory.Settings.AdoBatchSize;
            this.currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");

            // let's be optimistic :-)
            this.supportsBatching = true;
        }

        #endregion Constructors

        #region Properties

        /// <summary>
        /// Gets or sets the size of the batch, this can change dynamically by
        /// calling the session's SetBatchSize.
        /// </summary>
        /// <value>The size of the batch.</value>
        public override int BatchSize
        {
            get
            {
                return this.batchSize;
            }

            set
            {
                this.batchSize = value;
            }
        }

        /// <summary>
        /// Gets the count of statements in current batch.
        /// </summary>
        /// <value>The count of statements in current batch.</value>
        protected override int CountOfStatementsInCurrentBatch
        {
            get
            {
                return this.countOfStatementsInCurrentBatch;
            }
        }

        #endregion Properties

        #region Methods

        /// <summary>
        /// Adds the expected row count into the batch.
        /// </summary>
        /// <param name="expectation">The number of rows expected to be affected by the query.
        /// </param>
        /// <remarks>
        /// If Batching is not supported, then this is when the Command should be executed.  If 
        /// Batching is supported then it should hold off on executing the batch until explicitly 
        /// told to.
        /// </remarks>
        public override void AddToBatch(IExpectation expectation)
        {
            if (this.batchSize == 1) 
            {
                // batching not enabled for this configuration
                this.ExecuteBatchImmediate(expectation);
            }
            else if (this.currentBatch == null && 
                     !this.SupportedBatchingCommand(this.CurrentCommand))
            {
                // command cannot be batched (due to driver/server limitations) - or the driver 
                // does not support batching.
                this.ExecuteBatchImmediate(expectation);
            }
            else
            {
                // Track the expected number of rows
                this.totalExpectedRowsAffected += expectation.ExpectedRowCount;

                // do the actual batching.
                this.CreateOrUpdateBatch();
            }
        }

        /// <summary>
        /// Does the batch execution.
        /// </summary>
        /// <param name="ps">The current command.</param>
        protected override void DoExecuteBatch(IDbCommand ps)
        {
            if (this.currentBatch == null)
            {
                return;
            }

            Log.Info("Executing batch");
            this.LogCommands();

            // get it all ready to go
            this.CheckReaders();
            this.Prepare(this.currentBatch);

            // wing it and see if it works :-)
            var rowsAffected = this.currentBatch.ExecuteNonQuery();
            Expectations.VerifyOutcomeBatched(this.totalExpectedRowsAffected, rowsAffected);

            // clean up after ourselves (the base class will dispose all the commands)
            this.totalExpectedRowsAffected = 0;
            this.countOfStatementsInCurrentBatch = 0;
            this.currentBatch = null;
        }

        /// <summary>
        /// Logs all the commands in the batch.
        /// </summary>
        protected void LogCommands()
        {
            if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
            {
                Factory.Settings.SqlStatementLogger.LogBatchCommand(
                    this.currentBatchCommandsLog.ToString());

                // reset to get ready for next batch.
                this.currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");
            }
        }

        /// <summary>
        /// Logs the current command.
        /// </summary>
        /// <param name="isNewBatch">
        /// <see langword="true"/> if this is the first item in the batch; otherwise,
        /// <see langword="false"/>.
        /// </param>
        protected void LogCurrentCommand(bool isNewBatch)
        {
            var sqlStatementLogger = Factory.Settings.SqlStatementLogger;
            string lineWithParameters = null;

            if (sqlStatementLogger.IsDebugEnabled || Log.IsDebugEnabled)
            {
                var formatStyle = sqlStatementLogger.DetermineActualStyle(FormatStyle.Basic);

                lineWithParameters = formatStyle.Formatter.Format(
                    sqlStatementLogger.GetCommandLineWithParameters(this.CurrentCommand));

                this.currentBatchCommandsLog
                    .Append("command ")
                    .Append(this.countOfStatementsInCurrentBatch)
                    .Append(":")
                    .AppendLine(lineWithParameters);
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug((isNewBatch ? "Adding to batch: " : "Adding to existing batch") +
                    lineWithParameters);
            }
        }

        /// <summary>
        /// Calls the main batch's AddBatch() method (via reflection).
        /// </summary>
        private void AddBatch()
        {
            var objType = this.currentBatch.GetType();
            var methodInfo = objType.GetMethod("AddBatch");

            // call the add batch method.
            methodInfo.Invoke(this.currentBatch, null);
        }

        /// <summary>
        /// Creates a new batch or updates an existing batch.
        /// </summary>
        private void CreateOrUpdateBatch()
        {
            if (this.currentBatch == null)
            {
                // this is the first command in a set
                this.currentBatch = this.CurrentCommand;
                this.LogCurrentCommand(true);
                
                // We need to be sure we provide a connection for the first item -
                // otherwise the driver whines.
                var sessionConnection = this.ConnectionManager.GetConnection();
                if (this.currentBatch.Connection == null ||
                    this.currentBatch.Connection != sessionConnection)
                {
                    this.currentBatch.Connection = sessionConnection;
                }

                // Make sure we're participating in the transaction (otherwise we get a sync
                // error from the driver).
                this.ConnectionManager.Transaction.Enlist(this.currentBatch);
            }
            else
            {
                this.LogCurrentCommand(false);

                // transfer the CurrentCommand's parameters to the existing batch.
                //
                // note that this _cannot_ be a foreach loop because the db2 driver changes
                // the contents of the collection as we're transferring parameters (so you'd
                // get an enumeration exception).
                for (int i = 0, l = this.CurrentCommand.Parameters.Count; i < l; i++)
                {
                    this.currentBatch.Parameters.Add(this.CurrentCommand.Parameters[i]);
                }
            }

            // add the current parameters to the batch
            this.AddBatch();

            this.countOfStatementsInCurrentBatch++;
            if (this.countOfStatementsInCurrentBatch >= this.batchSize)
            {
                // we've batched enough commands - send them off to the DB.
                this.ExecuteBatchWithTiming(this.currentBatch);
            }
        }

        /// <summary>
        /// Returns a value indicating whether the specified IDbCommand implements a supported
        /// batching mechanism.
        /// </summary>
        /// <param name="iDbCommand">The command to check.</param>
        /// <returns>
        /// <see langword="true"/> if the command uses a supported batching mechanism;
        /// otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// Older versions of the iDB2 drivers do not support batching - however, newer versions
        /// do.  So, we check to see if the command has method called 'AddBatch' to determine
        /// if batching is supported.
        /// </remarks>
        private bool SupportedBatchingCommand(IDbCommand iDbCommand)
        {
            // shortcut evaluation (we set this below if we find out that the driver doesn't even
            // have a way of batching).
            if (!this.supportsBatching)
            {
                return false;
            }
            
            var objType = iDbCommand.GetType();

            // this is really ugly, but there's a bunch of different versions of the driver
            // and each one supports different batching capabilities.  
            //
            // The only other way is to try calling the 'AddBatch' method
            // and seeing if it throws an InvalidOperationException and then retrying it later
            // and that's even uglier...
            var methodInfo = objType.GetMethod("statementValidForAddBatch",
                BindingFlags.NonPublic | BindingFlags.Instance);

            if (methodInfo == null)
            {
                Log.Info("iDB2 Batching disabled - not supported by current driver");

                // save us work later
                this.supportsBatching = false;

                return false;
            }

            // Before continuing we need to be sure we provide a connection and prepare the 
            // command - otherwise the test always fails
            var sessionConnection = this.ConnectionManager.GetConnection();
            if (iDbCommand.Connection == null ||
                iDbCommand.Connection != sessionConnection)
            {
                iDbCommand.Connection = sessionConnection;
            }

            // Make sure we're participating in the transaction (otherwise we get a sync
            // error from the driver).
            this.ConnectionManager.Transaction.Enlist(iDbCommand);

            iDbCommand.Prepare();

            // now execute the method to see if this statement is a supported batching command
            if ((bool)methodInfo.Invoke(iDbCommand, null) == false)
            {
                Log.Info("iDB2 Batching disabled - not supported for current type of statement");

                return false;
            }

            return true;
        }

        /// <summary>
        /// Executes the batch immediately.
        /// </summary>
        /// <param name="expectation">The expectation.</param>
        private void ExecuteBatchImmediate(IExpectation expectation)
        {
            // Driver doesn't support batching, so we immediately execute the command
            var cmd = CurrentCommand;
            var rowCount = ExecuteNonQuery(cmd);
            expectation.VerifyOutcomeNonBatched(rowCount, cmd);
        }

        #endregion Methods
    }
}