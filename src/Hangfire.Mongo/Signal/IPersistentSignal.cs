using System;
using System.Threading;

namespace Hangfire.Mongo.Signal
{
    /// <summary>
    /// 
    /// </summary>
    public interface IPersistentSignal
    {

        /// <summary>
        /// Sets the signal
        /// </summary>
        /// <param name="name">
        /// Name of the signal to set.
        /// </param>
        void Set(string name);

        /// <summary>
        /// Wait for a signal with name.
        /// </summary>
        /// <param name="name">
        /// Name of the signal to wait for.
        /// </param>
        void Wait(string name);

        /// <summary>
        /// Wait for a signal with name.
        /// </summary>
        /// <param name="name">
        /// Name of the signal to wait for.
        /// </param>
        /// <param name="timeout">
        /// The timeout for the wait.
        /// </param>
        void Wait(string name, TimeSpan timeout);


        /// <summary>
        /// Wait for a signal with name.
        /// </summary>
        /// <param name="name">
        /// Name of the signal to wait for.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancelation token for canceling the blocking wait.
        /// </param>
        /// <exception cref="System.OperationCanceledException">
        /// Thrown if wait is cancelled
        /// </exception>
        void Wait(string name, CancellationToken cancellationToken);

        /// <summary>
        /// Wait for a signal with one of the names.
        /// </summary>
        /// <param name="names">
        /// Names of the signal to wait for.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancelation token for canceling the blocking wait.
        /// </param>
        /// <exception cref="System.OperationCanceledException">
        /// Thrown if wait is cancelled
        /// </exception>
        void Wait(string[] names, CancellationToken cancellationToken);

    }
}