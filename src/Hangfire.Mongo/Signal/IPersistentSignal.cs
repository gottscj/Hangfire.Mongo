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
        /// <returns>
        /// True is name was set or
        /// false on timeout
        /// </returns>
        bool Wait(string name, TimeSpan timeout);


        /// <summary>
        /// Wait for a signal with name.
        /// </summary>
        /// <param name="name">
        /// Name of the signal to wait for.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancelation token for canceling the blocking wait.
        /// </param>
        /// <returns>
        /// True is name was set or
        /// false if cancellationTokem was canceled (timed out)
        /// </returns>
        bool Wait(string name, CancellationToken cancellationToken);

        /// <summary>
        /// Wait for a signal with one of the names.
        /// </summary>
        /// <param name="names">
        /// Names of the signal to wait for.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancelation token for canceling the blocking wait.
        /// </param>
        /// <returns>
        /// True if one of the names was set.
        /// False if cancellationTokem was canceled (timed out)
        /// </returns>
        bool Wait(string[] names, CancellationToken cancellationToken);

    }
}