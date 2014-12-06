using System;

namespace Hangfire.Mongo
{
	public class MongoStorageOptions
	{
		private readonly string _clientId = null;

		private TimeSpan _queuePollInterval;

		private TimeSpan _distributedLockLifetime;

		public MongoStorageOptions()
		{
			Prefix = "hangfire";
			QueuePollInterval = TimeSpan.FromSeconds(15);
			InvisibilityTimeout = TimeSpan.FromMinutes(30);
			DistributedLockLifetime = TimeSpan.FromSeconds(30);

			_clientId = Guid.NewGuid().ToString().Replace("-", String.Empty);
		}

		public string Prefix { get; set; }

		public TimeSpan QueuePollInterval
		{
			get { return _queuePollInterval; }
			set
			{
				var message = String.Format(
					"The QueuePollInterval property value should be positive. Given: {0}.",
					value);

				if (value == TimeSpan.Zero)
				{
					throw new ArgumentException(message, "value");
				}
				if (value != value.Duration())
				{
					throw new ArgumentException(message, "value");
				}

				_queuePollInterval = value;
			}
		}

		public TimeSpan InvisibilityTimeout { get; set; }

		public TimeSpan DistributedLockLifetime
		{
			get { return _distributedLockLifetime; }
			set
			{
				var message = String.Format(
					"The DistributedLockLifetime property value should be positive. Given: {0}.",
					value);

				if (value == TimeSpan.Zero)
				{
					throw new ArgumentException(message, "value");
				}
				if (value != value.Duration())
				{
					throw new ArgumentException(message, "value");
				}

				_distributedLockLifetime = value;
			}
		}

		public string ClientId
		{
			get { return _clientId; }
		}
	}
}