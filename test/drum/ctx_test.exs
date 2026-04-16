defmodule Drum.CtxTest do
  use ExUnit.Case, async: true
  import Drum.Test.PipelineHelpers

  describe "new/1: initial ctx" do
    test "initial ctx is passed to the first step" do
      test_pid = self()

      {:ok, pid} =
        Drum.new(%{hello: :world})
        |> Drum.step("step1", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
        |> run_pipeline()

      assert_receive {:ctx, %{hello: :world}}
      assert {:ok, _} = await_pipeline(pid)
    end
  end

  describe "ctx_add" do
    test "ctx_add merges keys into ctx for the next step" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.step("step1", fn _ctx, _cmd_opts -> {:ctx_add, %{key: :value}} end)
        |> Drum.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
        |> run_pipeline()

      assert_receive {:ctx, %{key: :value}}
      assert {:ok, _} = await_pipeline(pid)
    end

    test "ctx_add with conflicting keys fails the pipeline" do
      {:ok, pid} =
        Drum.new(%{key: :original})
        |> Drum.step("step1", fn _ctx, _cmd_opts -> {:ctx_add, %{key: :new}} end)
        |> run_pipeline()

      assert {:error, _} = await_pipeline(pid)
    end
  end

  describe "ctx_set" do
    test "ctx_set replaces ctx entirely for the next step" do
      test_pid = self()

      {:ok, pid} =
        Drum.new(%{old: :key})
        |> Drum.step("step1", fn _ctx, _cmd_opts -> {:ctx_set, %{fresh: :ctx}} end)
        |> Drum.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
        |> run_pipeline()

      assert_receive {:ctx, received_ctx}
      assert received_ctx == %{fresh: :ctx}
      assert {:ok, _} = await_pipeline(pid)
    end

    test "ctx_set preserves raw key" do
      test_pid = self()

      {:ok, pid} =
        Drum.new(%{raw: %{argv: []}, old: :key})
        |> Drum.step("step1", fn _ctx, _cmd_opts -> {:ctx_set, %{fresh: :ctx}} end)
        |> Drum.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
        |> run_pipeline()

      assert_receive {:ctx, received_ctx}
      assert received_ctx == %{fresh: :ctx, raw: %{argv: []}}
      assert {:ok, _} = await_pipeline(pid)
    end
  end

  describe "step return value" do
    test "non-matching step return value leaves ctx unchanged" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.step("step1", fn _ctx, _cmd_opts -> :some_other_value end)
        |> Drum.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
        |> run_pipeline()

      assert_receive {:ctx, %{}}
      assert {:ok, _} = await_pipeline(pid)
    end
  end
end
