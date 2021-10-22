using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
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
        private object writeLock = new object();

        internal int NumFrames { get; private set; }

        internal Func<ReadOnlyMemory<byte>, Task> CommandCallback { get; set; }

        private Connection(Socket socket, Func<ReadOnlyMemory<byte>, Task> callback)
        {
            this.socket = socket;
            CommandCallback = callback;
            var stream = new NetworkStream(socket);
            writer = PipeWriter.Create(stream);
            reader = PipeReader.Create(stream);
            readerTask = Task.Run(ProcessIncomingFrames);
        }

        public static async Task<Connection> Create(EndPoint ipEndpoint, Func<ReadOnlyMemory<byte>, Task> commandCallback)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            //TODO: make configurable
            socket.SendBufferSize *= 10;
            socket.ReceiveBufferSize *= 10;
            await socket.ConnectAsync(ipEndpoint);
            return new Connection(socket, commandCallback);
        }

        public async ValueTask<bool> Write<T>(T command) where T : struct, IClientCommand
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

        private void WriteCommand<T>(T command) where T : struct, IClientCommand
        {
            // Only one thread should be able to write to the output pipeline at a time.
            lock (writeLock)
            {
                var size = 4 + command.SizeNeeded; // + 4 to write the key and version
                int memSize = 4 + size; // + 4 to write the size
                var mem = writer.GetSpan(memSize);
                WireFormatting.WriteUInt64(mem, (((ulong)size) << 32) | (((ulong)command.Key) << 16) | (command.Version));
                var written = command.Write(mem.Slice(8));
                Debug.Assert(size == written);
                writer.Advance(memSize);
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
                while (TryReadFrame(ref buffer, out ReadOnlySequence<byte> frame))
                {
                    // Let's rent some memory to copy the frame from the network stream. This memory will be reclaimed once the frame has been handled.
                    byte[] array = ArrayPool<byte>.Shared.Rent((int)frame.Length);
                    Memory<byte> memory = array.AsMemory(0, (int)frame.Length);
                    frame.CopyTo(memory.Span);

                    // Let's advance the buffer, that gives the network IO a chance to read more things while we are processing this frame.
                    reader.AdvanceTo(buffer.Start, buffer.End);

                    var task = CommandCallback(memory);

                    // It's very likely that this completes synchronously if this isn't a Deliver command for example.
                    if (!task.IsCompleted)
                    {
                        // Didn't complete synchronously, let's await it.
                        await task.ConfigureAwait(false);
                    }

                    ArrayPool<byte>.Shared.Return(array);
                    NumFrames += 1;
                }
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
            if (!readerTask.IsCompleted)
            {
                readerTask.Wait();
            }

            socket.Dispose();
        }
    }
}
