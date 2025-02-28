using System;
using System.Collections.Generic;
using System.Linq;
using MediaBrowser.Controller.Configuration;
using Microsoft.Extensions.Logging;

namespace MediaBrowser.Controller.MediaStreaming
{
    /// <summary>
    /// Service responsible for managing all active streams and allocating bitrates
    /// based on the global streaming bitrate limit.
    /// </summary>
    public class GlobalStreamManager : IGlobalStreamManager
    {
        private readonly ILogger<GlobalStreamManager> _logger;
        private readonly IServerConfigurationManager _config;
        private readonly object _streamLock = new object();
        private readonly Dictionary<string, StreamInfo> _activeStreams = new Dictionary<string, StreamInfo>();

        public GlobalStreamManager(
            ILogger<GlobalStreamManager> logger,
            IServerConfigurationManager config)
        {
            _logger = logger;
            _config = config;
        }

        /// <summary>
        /// Registers a stream with the global manager.
        /// </summary>
        /// <param name="streamId">Unique identifier for the stream</param>
        /// <param name="initialBitrate">Initial bitrate in bits per second</param>
        /// <param name="minBitrate">Minimum acceptable bitrate</param>
        /// <param name="maxBitrate">Maximum desired bitrate</param>
        /// <returns>The allocated bitrate</returns>
        public int RegisterStream(string streamId, int initialBitrate, int minBitrate, int maxBitrate)
        {
            lock (_streamLock)
            {
                _activeStreams[streamId] = new StreamInfo
                {
                    StreamId = streamId,
                    CurrentBitrate = initialBitrate,
                    MinBitrate = minBitrate,
                    MaxBitrate = maxBitrate,
                    IsActive = true
                };
                
                _logger.LogDebug("Stream {StreamId} registered with initial bitrate {Bitrate}", streamId, initialBitrate);
                
                // Reallocate bitrates for all streams
                ReallocateBitrates();
                
                return _activeStreams[streamId].CurrentBitrate;
            }
        }

        /// <summary>
        /// Updates an existing stream's parameters.
        /// </summary>
        public int UpdateStream(string streamId, int requestedBitrate, int minBitrate, int maxBitrate)
        {
            lock (_streamLock)
            {
                if (_activeStreams.TryGetValue(streamId, out var stream))
                {
                    stream.RequestedBitrate = requestedBitrate;
                    stream.MinBitrate = minBitrate;
                    stream.MaxBitrate = maxBitrate;
                    
                    // Reallocate bitrates for all streams
                    ReallocateBitrates();
                    
                    return stream.CurrentBitrate;
                }
                
                return requestedBitrate; // If stream not found, just return the requested bitrate
            }
        }

        /// <summary>
        /// Unregisters a stream when it's no longer active.
        /// </summary>
        public void UnregisterStream(string streamId)
        {
            lock (_streamLock)
            {
                _activeStreams.Remove(streamId);
                _logger.LogDebug("Stream {StreamId} unregistered", streamId);
                
                // Reallocate bitrates for remaining streams
                ReallocateBitrates();
            }
        }

        /// <summary>
        /// Gets the current allocated bitrate for a stream.
        /// </summary>
        public int GetAllocatedBitrate(string streamId)
        {
            lock (_streamLock)
            {
                if (_activeStreams.TryGetValue(streamId, out var stream))
                {
                    return stream.CurrentBitrate;
                }
                
                return 0;
            }
        }

        /// <summary>
        /// Core algorithm to reallocate bitrates across all active streams.
        /// </summary>
        private void ReallocateBitrates()
        {
            // Get the global limit from network configuration
            var globalLimit = _config.Configuration.NetworkConfiguration.GlobalStreamingBitrateLimit;
            
            // If global limit is 0 (unlimited) or there are no active streams, no reallocation needed
            if (globalLimit <= 0 || _activeStreams.Count == 0)
            {
                foreach (var stream in _activeStreams.Values.Where(s => s.IsActive))
                {
                    // Use the requested bitrate or max bitrate if no specific request
                    stream.CurrentBitrate = stream.RequestedBitrate > 0 ? 
                        Math.Min(stream.RequestedBitrate, stream.MaxBitrate) : 
                        stream.MaxBitrate;
                }
                
                return;
            }
            
            // Start by allocating minimum bitrates
            var remainingBandwidth = globalLimit;
            var streamsToAllocate = _activeStreams.Values.Where(s => s.IsActive).ToList();
            
            foreach (var stream in streamsToAllocate)
            {
                stream.CurrentBitrate = stream.MinBitrate;
                remainingBandwidth -= stream.MinBitrate;
            }
            
            // If we can't even meet minimum requirements, we have to stick with minimums
            if (remainingBandwidth <= 0)
            {
                _logger.LogWarning("Global bitrate limit {Limit} is too low to meet minimum requirements", globalLimit);
                return;
            }
            
            // Distribute remaining bandwidth fairly
            while (remainingBandwidth > 0 && streamsToAllocate.Any())
            {
                var streamCount = streamsToAllocate.Count;
                var bitratePerStream = remainingBandwidth / streamCount;
                
                if (bitratePerStream <= 0)
                {
                    break;
                }
                
                var fulfilledStreams = new List<StreamInfo>();
                
                foreach (var stream in streamsToAllocate)
                {
                    var targetBitrate = stream.RequestedBitrate > 0 ? 
                        Math.Min(stream.RequestedBitrate, stream.MaxBitrate) : 
                        stream.MaxBitrate;
                    
                    var additionalNeeded = targetBitrate - stream.CurrentBitrate;
                    
                    if (additionalNeeded <= 0)
                    {
                        // Stream is already at its target
                        fulfilledStreams.Add(stream);
                    }
                    else if (additionalNeeded <= bitratePerStream)
                    {
                        // Stream can be fully satisfied
                        remainingBandwidth -= additionalNeeded;
                        stream.CurrentBitrate = targetBitrate;
                        fulfilledStreams.Add(stream);
                    }
                    else
                    {
                        // Partially fulfill this stream
                        remainingBandwidth -= bitratePerStream;
                        stream.CurrentBitrate += bitratePerStream;
                    }
                }
                
                // Remove fulfilled streams from consideration
                foreach (var stream in fulfilledStreams)
                {
                    streamsToAllocate.Remove(stream);
                }
                
                // If no streams were fulfilled in this iteration, we're stuck
                if (fulfilledStreams.Count == 0 && streamsToAllocate.Count > 0)
                {
                    // Distribute remaining evenly
                    bitratePerStream = remainingBandwidth / streamsToAllocate.Count;
                    foreach (var stream in streamsToAllocate)
                    {
                        stream.CurrentBitrate += bitratePerStream;
                    }
                    break;
                }
            }
            
            _logger.LogDebug("Bitrates reallocated among {Count} streams with global limit {Limit}", 
                _activeStreams.Count, globalLimit);
        }

        /// <summary>
        /// Internal class to store stream information.
        /// </summary>
        private class StreamInfo
        {
            public string StreamId { get; set; }
            public int CurrentBitrate { get; set; }
            public int RequestedBitrate { get; set; }
            public int MinBitrate { get; set; }
            public int MaxBitrate { get; set; }
            public bool IsActive { get; set; } = true;
        }
    }

    /// <summary>
    /// Interface for the global stream manager.
    /// </summary>
    public interface IGlobalStreamManager
    {
        int RegisterStream(string streamId, int initialBitrate, int minBitrate, int maxBitrate);
        int UpdateStream(string streamId, int requestedBitrate, int minBitrate, int maxBitrate);
        void UnregisterStream(string streamId);
        int GetAllocatedBitrate(string streamId);
    }
}
