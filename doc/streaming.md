# Creating a Source

## Example

```cpp

```

## Step-by-Step

The member function `scheduled_actor::make_source` creates a `stream_manager`
and returns it, wrapped into an `annotated_stream<...>`. Users are supposed to
return this from the message handler for two reasons:

  1. Type checking. Composing actors that operate on streams automagically
     creates a pipeline.
  2. Handshaking. The `scheduled_actor` passes the `stream<...>` object to the
     `default_invoke_result_handler`. The handler unwraps the `stream_manager`
     and calls `self->connect_pipeline(manager)`.

