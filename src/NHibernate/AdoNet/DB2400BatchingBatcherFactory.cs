using NHibernate.Engine;

namespace NHibernate.AdoNet
{
    /// <summary>
    /// A batcher factory for iDB2 (DB2 for iSeries).
    /// </summary>
    public class DB2400BatchingBatcherFactory : IBatcherFactory
    {
        /// <summary>
        /// Creates the batcher.
        /// </summary>
        /// <param name="connectionManager">The connection manager.</param>
        /// <param name="interceptor">The interceptor.</param>
        /// <returns>A batcher for the specified connection manager.</returns>
        public virtual IBatcher CreateBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
        {
            return new DB2400BatchingBatcher(connectionManager, interceptor);
        }        
    }
}