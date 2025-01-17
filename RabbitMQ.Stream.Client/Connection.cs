using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public class Connection : IDisposable
    {
        private readonly Socket socket;
        private readonly PipeWriter writer;
        private readonly PipeReader reader;
        private readonly Task readerTask;
        private Func<Memory<byte>, Task> commandCallback;

        private int numFrames;
        private object writeLock = new object();

        internal int NumFrames => numFrames;

        internal Func<Memory<byte>, Task> CommandCallback
        {
            get => commandCallback;
            set => commandCallback = value;
        }

        private Connection(Socket socket, Func<Memory<byte>, Task> callback)
        {
            this.socket = socket;
            this.commandCallback = callback;
            var stream = new NetworkStream(socket);
            writer = PipeWriter.Create(stream);
            reader = PipeReader.Create(stream);
            readerTask = Task.Run(ProcessIncomingFrames);
        }

        public static async Task<Connection> Create(EndPoint ipEndpoint, Func<Memory<byte>, Task> commandCallback)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            //TODO: make configurable
            socket.SendBufferSize *= 10;
            socket.ReceiveBufferSize *= 10;
            await socket.ConnectAsync(ipEndpoint);
            return new Connection(socket, commandCallback);
        }

        public async ValueTask<bool> Write<T>(T command) where T : struct, ICommand
        {
            WriteCommand(command);
            var flushTask = writer.FlushAsync();

            // Let's check if this completed synchronously befor invoking the async state mahcine
            if (!flushTask.IsCompletedSuccessfully)
            {
                await flushTask.ConfigureAwait(false);
            }

            return flushTask.Result.IsCompleted;
        }

        private void WriteCommand<T>(T command) where T : struct, ICommand
        {
            // Only one thread should be able to write to the output pipeline at a time.
            lock (writeLock)
            {
                var size = command.SizeNeeded;
                var mem = writer.GetSpan(4 + size); // + 4 to write the size
                WireFormatting.WriteUInt32(mem, (uint)size);
                var written = command.Write(mem.Slice(4));
                Debug.Assert(size == written);
                writer.Advance(4 + written);
            }
        }

        private async Task ProcessIncomingFrames()
        {
            while (true)
            {
                if (!reader.TryRead(out ReadResult result))
                {
                    result = await reader.ReadAsync().ConfigureAwait(false);
                }

                var buffer = result.Buffer;
                if (buffer.Length == 0)
                {
                    // We're not going to receive any more bytes from the connection.
                    break;
                }

                // Let's try to read some frames!
                while(TryReadFrame(ref buffer, out ReadOnlySequence<byte> frame))
                {
                    // Let's rent some memory to copy the frame from the network stream. This memory will be reclaimed once the frame has been handled.
                    Memory<byte> memory = ArrayPool<byte>.Shared.Rent((int)frame.Length).AsMemory(0, (int)frame.Length);
                    frame.CopyTo(memory.Span);
                    await commandCallback(memory).ConfigureAwait(false);
                    this.numFrames += 1;
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }

            // Mark the PipeReader as complete.
            await reader.CompleteAsync();
        }

        private bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> frame)
        {
            // Do we have enough bytes in the buffer to begin parsing a frame?
            if (buffer.Length > 4)
            {
                // Let's see how big the next frame is
                WireFormatting.ReadUInt32(buffer, out var frameSize);
                if (buffer.Length >= 4 + frameSize)
                {
                    // We have enough bytes in the buffer to read a whole frame so let's read it
                    frame = buffer.Slice(4, frameSize);

                    // Let's slice the buffer at the end of the current frame
                    buffer = buffer.Slice(frame.End);
                    return true;
                }
            }

            frame = ReadOnlySequence<byte>.Empty;
            return false;
        }

        public void Dispose()
        {
            writer.Complete();
            reader.Complete();
            socket.Dispose();
        }
    }
}
